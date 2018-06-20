package taskGraph;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.util.List;
import java.util.Map;

import taskGraph.resource.ResourceFactory;
import utils.Config;
import utils.Utils;


public class TaskFactory {

	private final Master master;
	private final ResourceTracker tracker;

	public TaskFactory(Master master, ResourceTracker tracker) {
		this.master = master;
		this.tracker = tracker;
	}

	/* ********************** *
	 * *** STATE MACHINE  *** *
	 * ********************** */

	public void splitInput(String path) {
		TaskNodeBuilder.newBuilder()
		.setResourceTracker(tracker)
		.setManager(master)
		.assignTask((listener) -> { 
			int nFile = Utils.divideFile(path, Config.DEFAULT_CHUNK_SIZE); 
			InputStream in = new ByteArrayInputStream(String.valueOf(nFile).getBytes()); // write nFile to listener
			listener.setInputStream(in);
		})
		.assignResultDeliverer( (n, messages) -> {
			int nFile = Integer.parseInt(messages.get(0));

			for(int k=0; k<nFile; k++) {
				String computerID = Utils.getAvailableComputer(master.getComputerPool());
				//				System.out.println("computer avaialble : "+ tracker.isAvailable(ResourceFactory.computer(computerID)));
				deploySplit(k, computerID);
				map(k, computerID);
			}

			mapSyncNode(nFile);
			//			System.err.println("the nodes have been created");
		})
		.build();

	}



	public TaskNode deploySplit(int splitID, String computerID) {
		// create node
		TaskNode n = TaskNodeBuilder.newBuilder()
				.setResourceTracker(tracker)
				.setManager(master)
				.addRequiredResource(ResourceFactory.computer(computerID))
				.addProducedResource(ResourceFactory.splitFile(splitID))
				.assignTask(() -> { return CommandLineAdapter.deploySplit(splitID, computerID);})
				.build();
		return n;
	}

	public TaskNode map(int splitID, String computerID) {
		TaskNode node =TaskNodeBuilder.newBuilder()
				.setResourceTracker(tracker)
				.setManager(master)
				.addRequiredResource(ResourceFactory.computer(computerID))
				.addRequiredResource(ResourceFactory.splitFile(splitID))
				.addProducedResource(ResourceFactory.mapFile(splitID))
				.assignTask(() -> { return CommandLineAdapter.startMapSlave(splitID, computerID); })
				.assignResultDeliverer((n, messages) -> { master.deliverMapKey(splitID, computerID, messages); })
				.build();
		return node;
	}

	public TaskNode mapSyncNode(int nFile) {
		// create node
		TaskNodeBuilder builder = TaskNodeBuilder.newBuilder()
				.setResourceTracker(tracker)
				.setManager(master);
		for(int k=0; k<nFile; k++) {
			builder.addRequiredResource(ResourceFactory.mapFile(k));
		}
		builder.assignResultDeliverer(() -> {master.notifyAllKeyCollected();});
		return builder.build();
	}

	public void shuffle(List<String> keys, Map<String, Map<String, List<Integer>>> invertedKeyMap) {
		System.err.println("Starting shuffle");
		for(String key : keys) {
			Map<String, List<Integer>> computerMapping = invertedKeyMap.get(key);
			for(Map.Entry<String, List<Integer>> e : computerMapping.entrySet()) {
				String sourceComputerID = e.getKey();
				List<Integer> mapIDs = e.getValue();
				int reduceId = keys.indexOf(key);
				keyExtraction(sourceComputerID, key, reduceId, mapIDs); // extract 'key' from mapIds files, and store in separate files in tmp folder
				String targetcomputerID = Utils.getAvailableComputer(master.getComputerPool());
				gatherReduceFile(reduceId, sourceComputerID, targetcomputerID); // gather tmp reduces files in a single computer
				concatenateReduceFiles(reduceId, targetcomputerID); // gather this computer tmp reduce files in a single file
			}
		}

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
	private TaskNode keyExtraction(String computerID, String key, int reduceID, List<Integer> mapIDs) {
		TaskNodeBuilder builder = TaskNodeBuilder.newBuilder()
				.setResourceTracker(tracker)
				.setManager(master);
		for(int mapID : mapIDs) {
			builder.addRequiredResource(ResourceFactory.mapFile(mapID));
		}
		builder.addProducedResource(ResourceFactory.reduceTmpFile(reduceID, computerID))
		.assignTask(() -> { return CommandLineAdapter.keyExtraction(computerID, key, reduceID, mapIDs); });
		return builder.build();
	}

	
	/**
	 * Cette fonction copie le fichier reduce d'identifiant reduceID et situé sur l'ordinateur
	 * sourceComputerID sur l'ordinateur tartgetComputerId dans le dossier tmp dédié.
	 * 
	 * @param reduceID : identifiant du fichier reduce source
	 * @param sourceComputerID : identifiant de l'ordinateur source
	 * @param targetComputerID : identifiant de l'ordinateur destination
	 */
	private TaskNode gatherReduceFile(int reduceID, String sourceComputerID, String targetComputerID) {
		TaskNode node =TaskNodeBuilder.newBuilder()
				.setResourceTracker(tracker)
				.setManager(master)
				.addRequiredResource(ResourceFactory.computer(sourceComputerID))
				.addRequiredResource(ResourceFactory.computer(targetComputerID))
				.addRequiredResource(ResourceFactory.reduceTmpFile(reduceID, sourceComputerID))
				.addProducedResource(ResourceFactory.reduceTmp2File(reduceID, targetComputerID))
				.assignTask(() -> { return CommandLineAdapter.gatherReduceFiles(reduceID, sourceComputerID, targetComputerID); })
				.build();
		return node;
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
	private TaskNode concatenateReduceFiles(int reduceID, String computerID) {
		TaskNode node =TaskNodeBuilder.newBuilder()
				.setResourceTracker(tracker)
				.setManager(master)
				.addRequiredResource(ResourceFactory.computer(computerID))
				.addRequiredResource(ResourceFactory.reduceTmp2File(reduceID, computerID))
				.addProducedResource(ResourceFactory.reduceFile(reduceID))
				.assignTask(() -> { return CommandLineAdapter.concatenateReduceFiles(reduceID, computerID); })
				.assignResultDeliverer(() -> {reduce(reduceID, computerID);})
				.build();
		return node;
	}


	public TaskNode reduce(int reduceID, String computerID) {
		TaskNode node =TaskNodeBuilder.newBuilder()
				.setResourceTracker(tracker)
				.setManager(master)
				.addRequiredResource(ResourceFactory.computer(computerID))
				.addRequiredResource(ResourceFactory.reduceFile(reduceID))
				.assignTask(() -> { return CommandLineAdapter.startReduceSlave(reduceID, computerID); })
				.assignResultDeliverer((n, messages) -> { master.deliverReduceResult(reduceID, messages); })
				.build();
		return node;
	}




	/* ***************************** *
	 * *** DEPLOY COMPUTER LOGIC *** *
	 * ***************************** */

	public void checkComputer(String computerID) {
		//		System.err.println("check computer method called");
		TaskNodeBuilder.newBuilder()
		.setResourceTracker(tracker)
		.setManager(master)
		.setTimeOut(Config.DEFAULT_COMPUTER_REACHABILITY_CHECK_TIMEOUT) // accelerate time out of process to check computer more rapidly
		.assignTask(() -> { return CommandLineAdapter.checkConnectivity(computerID);})
		.assignResultDeliverer(() -> {master.getComputerPool().setComputerAvailable(computerID);})
		.build();
	}

	public void deployToComputer(String computerID) {
		prepareRootDirectory(computerID);
	}
	private void prepareRootDirectory(String computerID) {
		TaskNodeBuilder.newBuilder()
		.setResourceTracker(tracker)
		.setManager(master)
		.assignTask(() -> { return CommandLineAdapter.prepareRootDirectory(computerID);})
		.assignResultDeliverer( () -> {deleteFiles(computerID);})
		.build();
	}
	private void deleteFiles(String computerID) {
		TaskNodeBuilder.newBuilder()
		.setResourceTracker(tracker)
		.setManager(master)
		.assignTask(() -> { return CommandLineAdapter.deleteFiles(computerID);})
		.assignResultDeliverer( () -> {transferSlaveJar(computerID);})
		.build();
	}
	private void transferSlaveJar(String computerID) {
		TaskNodeBuilder.newBuilder()
		.setResourceTracker(tracker)
		.setManager(master)
		.assignTask(() -> { return CommandLineAdapter.transferSlaveJar(computerID);})
		.assignResultDeliverer( () -> {prepareSplitDirectory(computerID);})
		.build();
	}
	private void prepareSplitDirectory(String computerID) {
		TaskNodeBuilder.newBuilder()
		.setResourceTracker(tracker)
		.setManager(master)
		.assignTask(() -> { return CommandLineAdapter.prepareSplitDirectory(computerID);})
		.assignResultDeliverer( () -> {prepareMapDirectory(computerID);})
		.build();
	}
	private void prepareMapDirectory(String computerID) {
		TaskNodeBuilder.newBuilder()
		.setManager(master)
		.assignTask(() -> { return CommandLineAdapter.prepareMapDirectory(computerID);})
		.assignResultDeliverer( () -> {prepareReduceDirectory(computerID);})
		.build();
	}
	private void prepareReduceDirectory(String computerID) {
		TaskNodeBuilder.newBuilder()
		.setManager(master)
		.assignTask(() -> { return CommandLineAdapter.prepareReduceDirectory(computerID);})
		.assignResultDeliverer( (s) -> {prepareReduceTmpDirectory(computerID);})
		.build();
	}
	private void prepareReduceTmpDirectory(String computerID) {
		TaskNodeBuilder.newBuilder()
		.setManager(master)
		.assignTask(() -> { return CommandLineAdapter.prepareReduceTmpDirectory(computerID);})
		.assignResultDeliverer( () -> {master.getComputerPool().setComputerReady(computerID);})
		.build();
	}

}
