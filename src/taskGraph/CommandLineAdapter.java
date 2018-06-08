package taskGraph;
import java.io.IOException;
import java.lang.ProcessBuilder.Redirect;
import java.util.List;

import utils.Config;

public class CommandLineAdapter {

	private final static boolean DEBUG = true;

	private final static String LOCAL_SPLIT_FOLDER = Config.LOCAL_SPLIT_FOLDER;
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
	 * et situé sur l'ordinateur mapComputerID vers l'ordinateur 
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
	 * @param reduceID : identifiant des fichiers reduce concatenés
	 * @param computerID : identifiant de l'ordinateur sur lequel les fichiers
	 * 	                   reduce d'identifiants reduceID ont préalablement été
	 * 					   rassemblés
	 */
	public static Process concatenateReduceFiles(int reduceID, String computerID) {
		String mode = "concatenate";
		String sourceDirectory = REMOTE_ROOT_FOLDER + "/" + REMOTE_REDUCE_TEMP_FOLDER + "/" + reduceID;  
		String targetFilename = REMOTE_ROOT_FOLDER + "/" + REMOTE_REDUCE_FOLDER + "/" + REDUCE_DEFAULT_FILENAME + reduceID;
		return startSlave(computerID, mode, sourceDirectory, targetFilename);
	}
	
	/**
	 * Cette fonction copie le fichier reduce d'identifiant reduceID et situé sur l'ordinateur
	 * sourceComputerID sur l'ordinateur tartgetComputerId dans le dossier tmp dédié.
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
	 * Cette fonction démarre un map slave sur le split d'identifiant
	 * splitID situé sur l'ordinateur d'identifiant computerID. 
	 * Ce processus créera un fichier map coresspondant dans le 
	 * dossier map
	 * 
	 * Remarque : Cette fonction suppose que le dossier map a déjà été créé.
	 * Le programme Deploy se charge de cette création (le cas échéant).
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
	 * Cette fonction démarre un shuffle slave qui parcourt les fichiers map
	 * d'identifiants mapIDs à la recherche des valeurs associées à la clé key.
	 * Il réunit ces valeurs en une liste qu'il écrit dans le fichier reduce
	 * d'identifiant reduceID
	 * 
	 * Remarque : Cette fonction suppose que le dossier reduce a déjà été créé
	 * Le programme Deploy se charge de cette création (le cas échéant).
	 * 
	 * @param computerID : le nom de l'ordinateur destination 
	 * @param key : la clé dont on rassemble les valeurs
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
	 * Cette fonction démarre un reduce slave lançant la fonction
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
	 * Remarque : Cette fonction suppose que le dossier destination a déjà été
	 * créé. Le programme Deploy se charge de cette création (le cas échéant).
	 * 
	 * @param computerID : le nom de l'ordinateur destination 
	 * @param splitID : le nom du split (sur l'ordinateur-Master)
	 */
	public static Process deploySplit(int splitID, String computerID) {
		String cmd;
		Process p = null;
		try {
			cmd =   // transfert depuis le master vers le serveur ssh de l'école
					"scp " 
					+ LOCAL_SPLIT_FOLDER + "/" + SPLIT_DEFAULT_FILENAME + splitID + " " 
					+ "dperera@ssh.enst.fr:" + REMOTE_TEMP_FOLDER + " "

					// si réussite, on enchaine avec la commande suivante
					+ "&& "

					// transfert depuis le dossier intermédiaire vers l'ordinateur destination
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
