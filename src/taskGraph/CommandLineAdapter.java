package taskGraph;
import java.io.IOException;
import java.lang.ProcessBuilder.Redirect;
import java.util.List;

import utils.Config;

public class CommandLineAdapter {

	private final static boolean DEBUG = true;

	private final static String LOCAL_SPLIT_FOLDER = Config.LOCAL_SPLIT_FOLDER;
	private final static String LOCAL_SLAVE_FOLDER = Config.LOCAL_SLAVE_FOLDER;
	private final static String REMOTE_TEMP_FOLDER = Config.REMOTE_TEMP_FOLDER;
	private final static String REMOTE_ROOT_FOLDER = Config.REMOTE_ROOT_FOLDER;
	private final static String REMOTE_SPLIT_FOLDER = Config.REMOTE_SPLIT_FOLDER;
	private final static String REMOTE_MAP_FOLDER = Config.REMOTE_MAP_FOLDER;
	private static final String REMOTE_REDUCE_FOLDER = Config.REMOTE_REDUCE_FOLDER;
	private static final String REMOTE_REDUCE_TEMP_FOLDER = Config.REMOTE_REDUCE_TEMP_FOLDER;
	private final static String SLAVE_FILENAME = Config.SLAVE_FILENAME;
	private final static String SPLIT_DEFAULT_FILENAME = Config.SPLIT_DEFAULT_FILENAME;
	private final static String MAP_DEFAULT_FILENAME = Config.MAP_DEFAULT_FILENAME;
	private static final String REDUCE_DEFAULT_FILENAME = Config.REDUCE_DEFAULT_FILENAME;




	/**
	 * Cette fonction copie le fichier map d'identifiant mapID 
	 * et situ� sur l'ordinateur mapComputerID vers l'ordinateur 
	 * d'identifiant reduceComputerID
	 * 
	 * @param computerID : le nom de l'ordinateur destination 
	 * @param splitID : l'identifiant split (sur l'ordinateur-Master)
	 */
	public static Process copyKeyValue(int mapID, String mapComputerID, String reduceComputerID) {
		String cmd = "ssh " 
				+ " dperera@ssh.enst.fr "
				+ "scp "
				+ mapComputerID + ":" + REMOTE_ROOT_FOLDER + "/" + REMOTE_MAP_FOLDER + "/" + MAP_DEFAULT_FILENAME + mapID + " " 
				+ reduceComputerID + ":" + REMOTE_ROOT_FOLDER + "/" + REMOTE_REDUCE_FOLDER + "/";
		Process p = null;
		try {
			p = executeCmd(cmd);
		} catch(IOException e) {
			e.printStackTrace();
		}
		return p;
	}

	/**
	 * Cette fonction concatene tous les fichiers reduce du dossier temporaire
	 * de l'ordinateur computer et d'identifiant reduceID en un seul
	 * 
	 * @param reduceID : identifiant des fichiers reduce concaten�s
	 * @param computerID : identifiant de l'ordinateur sur lequel les fichiers
	 * 	                   reduce d'identifiants reduceID ont pr�alablement �t�
	 * 					   rassembl�s
	 */
	public static Process concatenateReduceFiles(int reduceID, String computerID) {
		String mode = "concatenate";
		String sourceDirectory = REMOTE_ROOT_FOLDER + "/" + REMOTE_REDUCE_TEMP_FOLDER + "/" + reduceID;  
		String targetFilename = REMOTE_ROOT_FOLDER + "/" + REMOTE_REDUCE_FOLDER + "/" + REDUCE_DEFAULT_FILENAME + reduceID;
		return startSlave(computerID, mode, sourceDirectory, targetFilename);
	}

	/**
	 * Cette fonction copie le fichier reduce d'identifiant reduceID et situ� sur l'ordinateur
	 * sourceComputerID sur l'ordinateur tartgetComputerId dans le dossier tmp d�di�.
	 * 
	 * @param reduceID : identifiant du fichier reduce source
	 * @param sourceComputerID : identifiant de l'ordinateur source
	 * @param targetComputerID : identifiant de l'ordinateur destination
	 */
	public static Process gatherReduceFiles(int reduceID, String sourceComputerID, String targetComputerID) {
		String cmd;
		Process p = null;
		try {
			cmd = "ssh " 
					+ " dperera@ssh.enst.fr "

				// two commands chained from server
				+ "'"

				// create target directory if it doesn't exist
				+ "ssh " 
				+ targetComputerID + " "
				+ "'mkdir -p " + REMOTE_ROOT_FOLDER + "/" + REMOTE_REDUCE_TEMP_FOLDER + "/" + reduceID + "' " 


				// if creation succeeded
				+ "&& "

				// transfer file
				+ "scp " 
				+ sourceComputerID + ":" + REMOTE_ROOT_FOLDER + "/" + REMOTE_REDUCE_FOLDER + "/" + REDUCE_DEFAULT_FILENAME + reduceID + " " 
				+ targetComputerID + ":" + REMOTE_ROOT_FOLDER + "/" + REMOTE_REDUCE_TEMP_FOLDER + "/" + reduceID + "/" + sourceComputerID
				+ "'";

			p = executeCmd(cmd);
		} catch(IOException e) {
			e.printStackTrace();
		}
		return p;
	}

	/**
	 * Cette fonction d�marre un map slave sur le split d'identifiant
	 * splitID situ� sur l'ordinateur d'identifiant computerID. 
	 * Ce processus cr�era un fichier map coresspondant dans le 
	 * dossier map
	 * 
	 * Remarque : Cette fonction suppose que le dossier map a d�j� �t� cr��.
	 * Le programme Deploy se charge de cette cr�ation (le cas �ch�ant).
	 * 
	 * @param computerID : le nom de l'ordinateur destination 
	 * @param splitID : l'identifiant split (sur l'ordinateur-Master)
	 */
	public static Process startMapSlave(int splitID, String computerID) {
		String mode = "map";
		String splitFilename = REMOTE_ROOT_FOLDER + "/" + REMOTE_SPLIT_FOLDER + "/" + SPLIT_DEFAULT_FILENAME + splitID;  
		String mapFilename = REMOTE_ROOT_FOLDER + "/" + REMOTE_MAP_FOLDER + "/" + MAP_DEFAULT_FILENAME + splitID;
		return startSlave(computerID, mode, splitFilename, mapFilename);
	}

	/**
	 * Cette fonction d�marre un shuffle slave qui parcourt les fichiers map
	 * d'identifiants mapIDs � la recherche des valeurs associ�es � la cl� key.
	 * Il r�unit ces valeurs en une liste qu'il �crit dans le fichier reduce
	 * d'identifiant reduceID
	 * 
	 * Remarque : Cette fonction suppose que le dossier reduce a d�j� �t� cr��
	 * Le programme Deploy se charge de cette cr�ation (le cas �ch�ant).
	 * 
	 * @param computerID : le nom de l'ordinateur destination 
	 * @param key : la cl� dont on rassemble les valeurs
	 * @param reduceID l'identifiant reduce (pour le nom du fichier sortie)
	 * @param mapIDs : les identifiants des fichiers map lus par le shuffle slave
	 */
	public static Process keyExtraction(String computerID, String key, int reduceID, List<Integer> mapIDs) {
		String mode = "shuffle";
		String reduceFilename = REMOTE_ROOT_FOLDER + "/" + REMOTE_REDUCE_FOLDER + "/" + REDUCE_DEFAULT_FILENAME + reduceID;
		String[] args = new String[mapIDs.size()+3];
		args[0] = mode;
		args[1] = key;
		args[2] = reduceFilename;
		for(int k=0; k<mapIDs.size(); k++) {
			String mapFilename =  REMOTE_ROOT_FOLDER + "/" + REMOTE_MAP_FOLDER + "/" + MAP_DEFAULT_FILENAME + mapIDs.get(k);
			args[k+3] = mapFilename;
		}
		return startSlave(computerID, args);
	}

	/**
	 * Cette fonction d�marre un reduce slave lan�ant la fonction
	 * reduce sur le fichier reduce d'identifiant reduceID.
	 * 
	 * @param computerID : le nom de l'ordinateur destination 
	 * @param splitID : l'identifiant reduce
	 */
	public static Process startReduceSlave(int reduceID, String computerID) {
		String mode = "reduce";
		String reduceFilename = REMOTE_ROOT_FOLDER + "/" + REMOTE_REDUCE_FOLDER + "/" + REDUCE_DEFAULT_FILENAME + reduceID;
		return startSlave(computerID, mode, reduceFilename);
	}

	/**
	 * Cette fonction transfert le split d'id splitID vers l'ordinateur 
	 * computerName.
	 * 
	 * Remarque : Cette fonction suppose que le dossier destination a d�j� �t�
	 * cr��. Le programme Deploy se charge de cette cr�ation (le cas �ch�ant).
	 * 
	 * @param computerID : le nom de l'ordinateur destination 
	 * @param splitID : le nom du split (sur l'ordinateur-Master)
	 */
	public static Process deploySplit(int splitID, String computerID) {
		String cmd;
		Process p = null;
		try {
			cmd =   // transfert depuis le master vers le serveur ssh de l'�cole
					"scp " 
					+ LOCAL_SPLIT_FOLDER + "/" + SPLIT_DEFAULT_FILENAME + splitID + " " 
					+ "dperera@ssh.enst.fr:" + REMOTE_TEMP_FOLDER + " "

					// si r�ussite, on enchaine avec la commande suivante
					+ "&& "

					// transfert depuis le dossier interm�diaire vers l'ordinateur destination
					+ "ssh dperera@ssh.enst.fr scp "
					+ REMOTE_TEMP_FOLDER + "/" + SPLIT_DEFAULT_FILENAME + splitID + " " 
					+ computerID + ":" + REMOTE_ROOT_FOLDER + "/" + REMOTE_SPLIT_FOLDER + "/" + SPLIT_DEFAULT_FILENAME + splitID;
			p = executeCmd(cmd);
		} catch(IOException e) {
			e.printStackTrace();
		}
		return p;
	}

	private static Process startSlave(String computerID, String... args) {
		String cmd = "ssh " 
				+ " dperera@ssh.enst.fr "
				+ "ssh " + computerID + " "
				+ "java -jar " + REMOTE_ROOT_FOLDER + "/" + SLAVE_FILENAME;
		for(String arg : args) {
			cmd += " " + arg;
		}
		Process p = null;
		try {
			p = executeCmd(cmd);
		} catch(IOException e) {
			e.printStackTrace();
		}
		return p;
	}

	public static void deployToServer() throws IOException, InterruptedException {
		String cmd = "scp " 
				+ LOCAL_SLAVE_FOLDER + "/" + SLAVE_FILENAME 
				+ " dperera@ssh.enst.fr:" + REMOTE_TEMP_FOLDER;
		executeCmd(cmd);
	}

	/*
	public static void deployToFinalComputer(String computerName) throws IOException, InterruptedException {
		String directoryFilepath, cmd;

		// pr�paration du dossier root (dossier destination)
		directoryFilepath  = REMOTE_ROOT_FOLDER;
		prepareDirectory(computerName, directoryFilepath);

		// suppression de tous les fichiers dans le dossier root et ses fils
		cmd = "ssh dperera@ssh.enst.fr ssh "
				+ computerName + " "
				+ "find " + REMOTE_ROOT_FOLDER + " -type f -delete"; 
		executeCmd(cmd);

		// transfert depuis le dossier interm�diaire vers le dossier destination
		cmd = "ssh dperera@ssh.enst.fr scp "
				+ REMOTE_TEMP_FOLDER + "/" + SLAVE_FILENAME + " " 
				+ computerName + ":" + REMOTE_ROOT_FOLDER;
		executeCmd(cmd);

		// pr�paration du dossier splits 
		directoryFilepath = REMOTE_ROOT_FOLDER + "/" + REMOTE_SPLIT_FOLDER;
		prepareDirectory(computerName, directoryFilepath);

		// pr�paration du dossier map
		directoryFilepath = REMOTE_ROOT_FOLDER + "/" + REMOTE_MAP_FOLDER;
		prepareDirectory(computerName, directoryFilepath);

		// pr�paration du dossier reduce
		directoryFilepath = REMOTE_ROOT_FOLDER + "/" + REMOTE_REDUCE_FOLDER;
		prepareDirectory(computerName, directoryFilepath);
		// pr�paration du dossier tmp reduce
		directoryFilepath = REMOTE_ROOT_FOLDER + "/" + REMOTE_REDUCE_TEMP_FOLDER;
		prepareDirectory(computerName, directoryFilepath);
	}
	*/

	public static Process prepareRootDirectory(String computerName) {
		// pr�paration du dossier root (dossier destination)
		String directoryFilepath  = REMOTE_ROOT_FOLDER;
		return prepareDirectory(computerName, directoryFilepath);
	}

	public static Process deleteFiles(String computerName) {
		Process p = null;
		try {
			// suppression de tous les fichiers dans le dossier root et ses fils
			String cmd = "ssh dperera@ssh.enst.fr ssh "
					+ computerName + " "
					+ "find " + REMOTE_ROOT_FOLDER + " -type f -delete"; 
			p = executeCmd(cmd);
		} catch(IOException e) {
			e.printStackTrace();
		}
		return p;
		
	}
	
	public static Process transferSlaveJar(String computerName) {
		Process p = null;
		try {
			// transfert depuis le dossier interm�diaire vers le dossier destination
			String cmd = "ssh dperera@ssh.enst.fr scp "
					+ REMOTE_TEMP_FOLDER + "/" + SLAVE_FILENAME + " " 
					+ computerName + ":" + REMOTE_ROOT_FOLDER;
			p = executeCmd(cmd);
		} catch(IOException e) {
			e.printStackTrace();
		}
		return p;
		
	}

	public static Process prepareSplitDirectory(String computerName) {
		// pr�paration du dossier root (dossier destination)
		String directoryFilepath = REMOTE_ROOT_FOLDER + "/" + REMOTE_SPLIT_FOLDER;
		return prepareDirectory(computerName, directoryFilepath);
	}

	public static Process prepareMapDirectory(String computerName) {
		// pr�paration du dossier root (dossier destination)
		String directoryFilepath = REMOTE_ROOT_FOLDER + "/" + REMOTE_MAP_FOLDER;
		return prepareDirectory(computerName, directoryFilepath);
	}

	public static Process prepareReduceDirectory(String computerName) {
		// pr�paration du dossier root (dossier destination)
		String directoryFilepath = REMOTE_ROOT_FOLDER + "/" + REMOTE_REDUCE_FOLDER;
		return prepareDirectory(computerName, directoryFilepath);
	}

	public static Process prepareReduceTmpDirectory(String computerName) {
		// pr�paration du dossier root (dossier destination)
		String directoryFilepath = REMOTE_ROOT_FOLDER + "/" + REMOTE_REDUCE_TEMP_FOLDER;
		return prepareDirectory(computerName, directoryFilepath);
	}

	private static Process prepareDirectory(String computerName, String directoryFilepath) {
		Process p = null;
		try {
			String cmd = 
					// suppression des fichiers du meme nom 
					"ssh dperera@ssh.enst.fr ssh "
					+ computerName + " "
					+ "rm -f " + directoryFilepath
					+ " ; "
					// cr�ation du dossier
					+ "ssh dperera@ssh.enst.fr ssh "
					+ computerName + " "
					+ "mkdir -p " + directoryFilepath; // cr�e les dossiers parents si n�cessaire
			p = executeCmd(cmd);
		} catch(IOException e) {
			e.printStackTrace();
		}
		return p;
	}


	/*
	private static Process prepareDirectory(String computerName, String directoryFilepath) throws IOException, InterruptedException {
		// suppression des fichiers du meme nom 
		String cmd = "ssh dperera@ssh.enst.fr ssh "
				+ computerName + " "
				+ "rm -f " + directoryFilepath;
		executeCmd(cmd);

		// cr�ation du dossier
		cmd = "ssh dperera@ssh.enst.fr ssh "
				+ computerName + " "
				+ "mkdir -p " + directoryFilepath; // cr�e les dossiers parents si n�cessaire
		executeCmd(cmd);
	}
	 */

	public static Process checkConnectivity(String computerID) {
		String cmd;
		Process p = null;
		try {
			cmd =   "ssh dperera@ssh.enst.fr "
					+ "ssh -o StrictHostKeyChecking=no " // append hostkey if unknown
					+ computerID + " /bin/true"; 
			p = executeCmd(cmd);
		} catch(IOException e) {
			e.printStackTrace();
		}
		return p;
	}

	private static Process executeCmd(String cmd) throws IOException {
		if(DEBUG)
			System.err.println("executing command : " + cmd);
		ProcessBuilder pb = new ProcessBuilder("bash.exe", "-c", cmd);
		pb.redirectError(Redirect.INHERIT);
		//pb.redirectOutput(Redirect.INHERIT);
		Process p = pb.start();
		return p;
	}

}
