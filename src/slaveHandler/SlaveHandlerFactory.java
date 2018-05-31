package slaveHandler;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import master.Master;
import slave.CommandLineAdapter;
import slave.SlaveTask;
import slave.ThreadMessageListener;
import utils.Utils;

/**
 * This class encapsulate the declaration of HandlerTask objects,
 * and the call to CommandLineAdapter method
 *
 */
public class SlaveHandlerFactory {


	private static final boolean DEBUG = true;

	/* ************************** *
	 * *****  SPLIT METHOD  ***** *
	 * ************************** */

	public static SlaveHandler split(Master master, Map<Integer, String> splitAssignment) {
		ResultlessSlaveHandler handler = new ResultlessSlaveHandler(master, () -> {master.notifyEndSplit();});
		
		// define slaves tasks
		for(Map.Entry<Integer, String> e : splitAssignment.entrySet()) {
			int splitID = e.getKey();
			String computerID = e.getValue();

			SlaveTask task = new SlaveTask() {

				@Override
				public Process execute() {
					Process p = CommandLineAdapter.deploySplit(splitID, computerID);
					return p;
				}

				@Override
				public void notifyCompletion() {
					handler.notifySlaveCompletion();
				}

				@Override
				public void notifyProcessCrash() {
					master.notifyMapSlaveCrash(splitID);
				}

				@Override
				public void notifyComputerCrash() {
					master.notifyComputerCrash(computerID);
				}

			};
			handler.assignTask(task);
		}

		handler.start();
		return handler;
	}


	/* ************************ *
	 * *****  MAP METHOD  ***** *
	 * ************************ */

	public static SlaveHandler map(Master master, Map<Integer, String> splitAssignment) {
		
		
		IdentifiedSlaveHandler<Integer, List<String>> handler = new IdentifiedSlaveHandler<Integer, List<String>>(master, (e) -> master.notifyEndMap(e));
		

		// define slaves tasks
		for(Map.Entry<Integer, String> e : splitAssignment.entrySet()) {
			int splitID = e.getKey();
			String computerID = e.getValue();

			SlaveTask task = new SlaveTask() {
				ArrayList<String> keyList = new ArrayList<String>();

				@Override
				public Process execute() {
					Process p = CommandLineAdapter.startMapSlave(splitID, computerID);
					new ThreadMessageListener(keyList, p.getInputStream()).start();
					return p;
				}

				@Override
				public void notifyCompletion() {
					handler.notifySlaveCompletion(splitID, keyList);
				}

				@Override
				public void notifyProcessCrash() {
					master.notifyMapSlaveCrash(splitID);
				}

				@Override
				public void notifyComputerCrash() {
					master.notifyComputerCrash(computerID);
				}

			};
			handler.assignTask(task);
		}

		handler.start();
		return handler;
	}



	/* **************************** *
	 * *****  SHUFFLE METHOD  ***** *
	 * **************************** */

	public static SlaveHandler shuffle(Master master, Map<Integer, List<String>> keyMap, Map<Integer, String> splitAssignment, Map<String, String> keyAssignment, Map<String, Integer> keyID, Map<String, Map<String, List<Integer>>> invertedMapping) {
		// define shuffle as a 3-chained-steps process
		// 1 : extract value associated at a given key in map local files; concatenate these value in a single local
		// 2 : gather these local files at a single computer
		// 3 : concatenate previously created files at this computer

		if(DEBUG)
			Utils.printInvertedMapping(invertedMapping);

		if(DEBUG)
			Utils.printKeyID(keyID);

		// last step : concatenation of reduce files
		ResultlessSlaveHandler reduceFileConcatenationHandler = new ResultlessSlaveHandler(master, () -> {master.notifyEndShuffle();}); 
		assignReduceFileConcatenationSlaveTask(master, reduceFileConcatenationHandler, keyAssignment, keyID);
		// middle step : gathering reduce file at a single computer
		ResultlessSlaveHandler reduceFileGatheringHandler = new ResultlessSlaveHandler(master, () -> {reduceFileConcatenationHandler.start();});
		assignReduceFileGatheringSlaveTask(master, reduceFileGatheringHandler, invertedMapping, keyAssignment, keyID);
		// first step : extract value associated to key
		ResultlessSlaveHandler valueExtractionHandler = new ResultlessSlaveHandler(master, () -> {reduceFileGatheringHandler.start();});
		assignValueExtractionSlaveTask(master, valueExtractionHandler,  invertedMapping, keyID);
		valueExtractionHandler.start();
		return reduceFileConcatenationHandler;
	}




	private static void assignReduceFileConcatenationSlaveTask(Master master, ResultlessSlaveHandler handler, Map<String, String> keyAssignment, Map<String, Integer> keyID) {
		for(Entry<String, String> e : keyAssignment.entrySet()) {
			String key = e.getKey();
			String computerID = e.getValue();

			if(DEBUG)
				System.out.println("assigning computer " + computerID + " to key : " + key);

			int reduceID = keyID.get(key);
			SlaveTask task = createReduceFileConcatenationSlaveTask(master, handler, reduceID, computerID);
			handler.assignTask(task);
		}
	}


	private static SlaveTask createReduceFileConcatenationSlaveTask(Master master, ResultlessSlaveHandler handler, int reduceID,  String computerID) {
		SlaveTask task = new SlaveTask() {
			@Override
			public Process execute() {
				Process p = CommandLineAdapter.concatenateReduceFiles(reduceID, computerID);
				return p;
			}
			@Override
			public void notifyCompletion() {
				handler.notifySlaveCompletion();
			}
			@Override
			public void notifyProcessCrash() {
				master.notifyShuffleSlaveCrash(reduceID);
			}
			@Override
			public void notifyComputerCrash() {
				master.notifyComputerCrash(computerID);
			}
		};
		return task;
	}

	private static void assignReduceFileGatheringSlaveTask(Master master, ResultlessSlaveHandler handler, Map<String, Map<String, List<Integer>>> invertedMapping, Map<String, String> keyAssignment, Map<String, Integer> keyID) {
		
		// for each key merge the values of all the map files in which this key exist
		for(Entry<String, Map<String, List<Integer>>> e : invertedMapping.entrySet()) { // for each key
			String key = e.getKey(); // get key
			int reduceID = keyID.get(key); // get id of the key
			String targetComputerID = keyAssignment.get(key); // get the key to which this file will be transfered
			for(Entry<String, List<Integer>> e1 : e.getValue().entrySet()) { // for each computer containing this key
				String sourceComputerID = e1.getKey(); // get computer name
				SlaveTask task = createReduceFileGatheringSlaveTask(master, handler, reduceID, sourceComputerID, targetComputerID);
				handler.assignTask(task);
			}
		}
	}

	private static SlaveTask createReduceFileGatheringSlaveTask(Master master, ResultlessSlaveHandler handler, int reduceID, String sourceComputerID, String targetComputerID) {
		SlaveTask task = new SlaveTask() {
			@Override
			public Process execute() {
				Process p = CommandLineAdapter.gatherReduceFiles(reduceID, sourceComputerID, targetComputerID);
				return p;
			}
			@Override
			public void notifyCompletion() {
				handler.notifySlaveCompletion();
			}
			@Override
			public void notifyProcessCrash() {
				master.notifyShuffleSlaveCrash(reduceID);
			}
			@Override
			public void notifyComputerCrash() {
				master.notifyComputerCrash(sourceComputerID);
			}
		};
		return task;
	}


	private static void assignValueExtractionSlaveTask(Master master, ResultlessSlaveHandler handler, Map<String, Map<String, List<Integer>>> invertedMapping, Map<String, Integer> keyID) {
		
		// for each key merge the values of all the map files in which this key exist
		for(Entry<String, Map<String, List<Integer>>> e1 : invertedMapping.entrySet()) { // for each key
			String key = e1.getKey(); // get key
			int reduceID = keyID.get(key); // get id of the key
			for(Entry<String, List<Integer>> e2 : e1.getValue().entrySet()) { // for each computer containing this key
				String computerID = e2.getKey(); // get computer name
				List<Integer> mapIDs = e2.getValue(); // get list of id of files in this computer containing this key
				SlaveTask task = createKeyExtractionSlaveTask(master, handler, computerID, key, reduceID, mapIDs);
				handler.assignTask(task);
			}
		}
	}

	private static SlaveTask createKeyExtractionSlaveTask(Master master, ResultlessSlaveHandler handler, String computerID, String key, int reduceID, List<Integer> mapIDs) {
		SlaveTask task = new SlaveTask() {
			@Override
			public Process execute() {
				Process p = CommandLineAdapter.keyExtraction(computerID, key, reduceID, mapIDs);
				return p;
			}
			@Override
			public void notifyCompletion() {
				handler.notifySlaveCompletion();
			}
			@Override
			public void notifyProcessCrash() {
				master.notifyShuffleSlaveCrash(reduceID);
			}
			@Override
			public void notifyComputerCrash() {
				master.notifyComputerCrash(computerID);
			}
		};
		return task;
	}

	/*
	public static SlaveHandler shuffle(Master master, Map<Integer, List<String>> keyMap, Map<Integer, String> splitAssignment, Map<String, String> keyAssignment) {
		ReturnlessSlaveHandler handler = new ReturnlessSlaveHandler(master, () -> {master.notifyEndShuffle();});

		// define slaves tasks
		int uniqueID = 0;
		Map<String, List<Integer>> computerAssignments =  new HashMap<String, List<Integer>>(); // mapping : computer -> list of id of map files that it will receive
		for(Map.Entry<Integer, List<String>> e : keyMap.entrySet()) { // iterating through results of the map phase
			int splitID = e.getKey(); // id of a split file (also the id of the correspond map file)
			for(String key : e.getValue()) { // iterating key found in split of id splitID 
				String assignedComputer = keyAssignment.get(key); // get the computer assigned to this key by the Master
				List<Integer> assignedFiles = computerAssignments.get(assignedComputer); // get the list of the ID of the files already assigned to this computer
				String sourceComputer = splitAssignment.get(splitID); // the source computer ID (source means : on which computer file has been created and will be fetched)
				if(!assignedFiles.contains(splitID) && !sourceComputer.equals(assignedComputer)) { // if the computer has not already this file stored (already assigned or sourceComputer == destinationComputer) 
					assignedFiles.add(splitID); // actualize the mapping : computer -> list of id of map files that it will receive

					// !!!!  CAUTION 
					// computerAssignments.put(assignedComputer, assignedFiles); 

					// send him the map-file of id splitID
					SlaveTask task = sendMapFileToReduceComputerSlaveTask(master, handler, splitID, sourceComputer, assignedComputer, uniqueID++); // create corresponding task
					handler.assignTask(splitID, task); // add this task to the handler
				} // endif computer not yet assigned file splitID
			} // endfor browsing key found in splitID file
		} // endfor browsing result of the map phase

		handler.start();
		return handler;
	}

	private static SlaveTask sendMapFileToReduceComputerSlaveTask(Master master, ReturnlessSlaveHandler handler, int mapID, String mapComputerID, String reduceComputerID, int slaveID) {
		System.out.println("creating slave task");
		SlaveTask task = new SlaveTask() {

			@Override
			public Process execute() {
				Process p = CommandLineAdapter.copyKeyValue(mapID, mapComputerID, reduceComputerID);
				return p;
			}

			@Override
			public void notifyCompletion() {
				handler.notifySlaveCompletion(slaveID);
			}

			@Override
			public void notifyProcessCrash() {
				master.notifyShuffleSlaveCrash(mapID, reduceComputerID);
			}

			@Override
			public void notifyComputerCrash() {
				master.notifyComputerCrash(reduceComputerID);
			}

		};
		return task;
	}
	 */


	/* *************************** *
	 * *****  REDUCE METHOD  ***** *
	 * *************************** */


	public static SlaveHandler reduce(Master master, Map<String, String> keyAssignment, Map<String, Integer> keysID) {
		// define handler task
		IdentifiedSlaveHandler<String, Integer> handler = new IdentifiedSlaveHandler<String, Integer>(master, (e) -> master.notifyEndReduce(e));
		
		// define slaves tasks
		for(Entry<String, String> e : keyAssignment.entrySet()) {
			String key = e.getKey();
			String computerID = e.getValue();
			int reduceID = keysID.get(key);
			
			SlaveTask task = createReduceSlaveTask(master, handler, computerID, reduceID, key); 
			handler.assignTask(task);
		}

		handler.start();
		return handler;
	}
	
	private static SlaveTask createReduceSlaveTask(Master master, IdentifiedSlaveHandler<String, Integer> handler, String computerID, int reduceID, String key) {
		SlaveTask task = new SlaveTask() {
			ArrayList<String> valueListener = new ArrayList<String>();

			@Override
			public Process execute() {
				Process p = CommandLineAdapter.startReduceSlave(reduceID, computerID);
				new ThreadMessageListener(valueListener, p.getInputStream()).start();
				return p;
			}

			@Override
			public void notifyCompletion() {
				try {
					int reducedValue = Integer.parseInt(valueListener.get(0));
					handler.notifySlaveCompletion(key, reducedValue);
				} catch(ArrayIndexOutOfBoundsException e) {
					e.printStackTrace();
					notifyProcessCrash();
				}
			}

			@Override
			public void notifyProcessCrash() {
				master.notifyReduceSlaveCrash(reduceID);
			}

			@Override
			public void notifyComputerCrash() {
				master.notifyComputerCrash(computerID);
			}

		};
		return task;
	}
	
	
	/* ********************************** *
	 * *****  MISCELLANEOUS TASKS  ****** *
	 * ********************************** */



	public static SlaveHandler connectivityCheck(Master master, List<String> computerIDs) {
		// define handler task
		IdentifiedSlaveHandler<String, Boolean> handler = new IdentifiedSlaveHandler<String, Boolean>(master, (e) -> master.notifyEndConnectictivityCheck(e));
		
		// define slaves tasks
		for(String computerID : computerIDs) {
			SlaveTask task = new SlaveTask() {
				@Override
				public Process execute() {
					Process p = CommandLineAdapter.checkConnectivity(computerID);
					return p;
				}
				@Override
				public void notifyCompletion() {
					System.err.println(computerID + " is reachable");
					handler.notifySlaveCompletion(computerID, true);
				}
				@Override
				public void notifyProcessCrash() {
					System.err.println(computerID + " is not reachable");
					handler.notifySlaveCompletion(computerID, false);
				}
				@Override
				public void notifyComputerCrash() {
					System.err.println(computerID + " is not reachable");
					handler.notifySlaveCompletion(computerID, false);
				}
			};
			handler.assignTask(task);
		}
		handler.start();
		return handler;
	}
	
}
