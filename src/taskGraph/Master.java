package taskGraph;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import master.Config;
import utils.Utils;

public class Master implements GraphManager {

	Map<String, Map<String, List<Integer>>> invertedKeyMap; // key -> computerID -> Set[mapID]
	private final List<String> keys; // list of keys
	private final ComputerPool computerPool;
	private final TaskFactory taskFactory;
	private final ResourceTracker tracker;
	
	public Master() {
		tracker = new ResourceTracker();
		taskFactory = new TaskFactory(this, tracker);
		computerPool = new ComputerPool(taskFactory, tracker);
		invertedKeyMap = new HashMap<String, Map<String, List<Integer>>>();
		keys = new ArrayList<String>();
	}
	
	// start point
	public void start() {
		System.out.println("WAITING FOR COMPUTER POOL TO COLLECT COMPUTER");
		Utils.waitUntilNComputerReady(computerPool, 1); // wait until 4 computer ready
		System.out.println("STARTING ALGORITHM");
		taskFactory.splitInput(Config.DEFAULT_INPUT_FILEPATH);
	}
	
	// MAP synchronisation point
	
	public void deliverMapKey(int mapID, String computerID, List<String> messages) {
		
		for(String key : messages) {
			// actualize list of keys
			keys.add(key);
			
			// actualize invertedKeyMap
			Map<String, List<Integer>> computerMapping = invertedKeyMap.get(key);
			if(computerMapping==null) {
				computerMapping = new HashMap<String, List<Integer>>();
			}
			
			if(!computerMapping.containsKey(computerID)) {
				computerMapping.put(computerID, new ArrayList<Integer>());
			}
			List<Integer> mapIDs = computerMapping.get(computerID);
			if(!mapIDs.contains(mapID)) {
				mapIDs.add(mapID);
			}
			computerMapping.put(computerID, mapIDs);
			invertedKeyMap.put(key, computerMapping);
		}
	}

	public void notifyAllKeyCollected() {
		System.out.println("Printing keys");
		Utils.printKeys(keys);
		taskFactory.shuffle(keys, invertedKeyMap);
	}
	
	// Algorithm result
	
	public void deliverReduceResult(int reduceID, List<String> messages) {
		System.out.print("reduce file " + reduceID + ": ");
		for(String s : messages) {
			System.out.print(s);
		}
		System.out.println("");
	}
	
	@Override
	public void notifyCompletion() {
		// TODO Auto-generated method stub
		
	}

	
	
	@Override
	public void notifyTaskFailure() {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void notifyComputerCrash(String computerID) {
		computerPool.notifyComputerCrash(computerID);
	}

	



	public ComputerPool getComputerPool() {
		return computerPool;
	}



}
