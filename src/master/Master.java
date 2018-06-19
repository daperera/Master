package master;
import java.util.List;
import java.util.Map;

import slaveHandler.SlaveHandlerFactory;

public class Master {

	private LaunchSynchronizer launchSynchronizer;

	private List<String> computerPool;
	private TaskAssignmentManager manager;
	
	
	
	private String inputFilepath;
	private int chunkSize;
	
	public Master() {
		// synchronize launch condition before starting computation
		launchSynchronizer = new LaunchSynchronizer(this);
		manager = new TaskAssignmentManager(this);
		chunkSize = Config.DEFAULT_CHUNK_SIZE;
		inputFilepath = Config.DEFAULT_INPUT_FILEPATH;

		System.out.println("Checking available computers");
		SlaveHandlerFactory.connectivityCheck(this, Utils.defaultComputerPool());
	}

	/* *********************************** *
	 * **** ALGORITHM PARAMETRIZATION **** *
	 * *********************************** */
	
	public void setChunkSize(int chunkSize) {
		this.chunkSize = chunkSize;
	}
	
	public void setInputFilepath(String inputFilepath) {
		this.inputFilepath = inputFilepath;
	}

	/* *********************** *
	 * **** STATE MACHINE **** *
	 * *********************** */

	public void compute() {
		launchSynchronizer.requestComputation();
	}
	
	public void notifyStartSplit() {
		System.out.println("Starting split phase");
		divideFile(inputFilepath);
		assignSplits(); // assign split to remote computer
		startSplit();
	}

	public void notifyEndSplit() {
		System.out.println("Starting map phase");
		startMap(); // start map step on remote computer 
	}


	public void notifyEndMap(Map<Integer, List<String>> keyMap) {
		System.out.println("Starting shuffle phase");
		assignKeys(keyMap);
		startShuffle(keyMap); // start shuffle step 
	}

	public void notifyEndShuffle() {
		System.out.println("Starting reduce phase");
		startReduce(); // start reduce step
	}

	public void notifyEndReduce(Map<String, Integer> reducedValues) {
		Utils.printReducedValues(reducedValues); // print result of the algorithm
	}

	
	
	/* ************************* *
	 * **** START FUNCTIONS **** *
	 * ************************* */

	private void startSplit() {
		Map<Integer, String> splitAssignment = manager.getSplitAssignment();
		SlaveHandlerFactory.split(this, splitAssignment);
	}

	private void startMap() {
		Map<Integer, String> splitAssignment = manager.getSplitAssignment();
		SlaveHandlerFactory.map(this, splitAssignment); // start reduce step on remote computer 
	}

	private void startShuffle(Map<Integer, List<String>> keyMap) {
		Map<Integer, String> splitAssignment = manager.getSplitAssignment();
		Map<String, String> keyAssignment = manager.getKeyAssignment();
		Map<String, Integer> keysID = manager.getKeysID();
		Map<String, Map<String, List<Integer>>> keyTocomputerIDToSplitIDList = manager.getKeyTocomputerIDToSplitIDList(); 
		SlaveHandlerFactory.shuffle(this, keyMap, splitAssignment, keyAssignment, keysID, keyTocomputerIDToSplitIDList);
	}
	
	private void startReduce() {
		Map<String, String> keyAssignment = manager.getKeyAssignment();
		Map<String, Integer> keysID = manager.getKeysID();
		SlaveHandlerFactory.reduce(this, keyAssignment, keysID);
	}	

	
	
	/* ****************************** *
	 * **** ASSIGNMENT FUNCTIONS **** *
	 * ****************************** */

	private void assignSplits() {
		manager.assignSplits();
	}

	private void assignKeys(Map<Integer, List<String>> keyMap) {
		manager.assignKeys(keyMap);
	}

	
	
	/* ************************ *
	 * **** CRASH HANDLING **** *
	 * ************************ */

	public void notifySplitSlaveCrash(int ID) {
		// TODO Auto-generated method stub
	}

	public void notifyMapSlaveCrash(int ID) {
		// TODO Auto-generated method stub
	}

	public void notifyShuffleSlaveCrash(int ID) {
		// TODO Auto-generated method stub
	}

	public void notifyReduceSlaveCrash(int ID) {
		// TODO Auto-generated method stub
	}


	public void notifyComputerCrash(String computerName) {
		// TODO Auto-generated method stub
	}



	/* ********************************* *
	 * **** MISCELLANEOUS FUNCTIONS **** *
	 * ********************************* */
	
	private void divideFile(String inputFilepath) {
		int splitNb = Utils.divideFile(inputFilepath, chunkSize);
		manager.setSplitNb(splitNb);
	}
	
	public void notifyEndConnectictivityCheck(Map<String, Boolean> computerAvailability) {
		System.out.println("Connectivity check ended");
		this.computerPool = Utils.extractAvailableComputerList(computerAvailability); // save result
		launchSynchronizer.notifyConnectivityCheckEnded(); // notify launchSynchronizer
	}


	
	/* ***************** *
	 * **** GETTERS **** *
	 * ***************** */

	public List<String> getComputerPool() {
		return computerPool;
	}

}