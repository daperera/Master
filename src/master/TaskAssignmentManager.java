package master;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import utils.Utils;

public class TaskAssignmentManager {

	// master class
	private final Master master;

	private int splitNb;

	/**
	 * Primary mapping
	 * 
	 * @keysID :  key -> keyID
	 * @splitAssignment : splitID -> computer to which it will be mapped
	 * @keyAssignemnt : key -> computer to which it will be reduced
	 * @keyMap : splitID -> List of the keys it contains
	 */
	private Map<String, Integer> keysID;
	private Map<Integer, String> splitAssignment;
	private Map<String, String> keyAssignment; 
	private Map<Integer, List<String>> splitKeys;


	/**
	 *  Secondary mapping : other views of the 
	 *  same data saved in this class for convenience.
	 *  
	 *  @keyTocomputerIDToSplitIDList : key -> computer in which split files are stored
	 *  									   that contain this key
	 *  									-> list of split files ID stored on this computer
	 *  									   that contains this key
	 */
	private Map<String, Map<String, List<Integer>>> keyTocomputerIDToSplitIDList;


	public TaskAssignmentManager(Master master) {
		this.master = master;
		keyAssignment = new HashMap<String, String>();
		splitAssignment = new HashMap<Integer, String>();
		keysID = new HashMap<String, Integer>();
		keyTocomputerIDToSplitIDList = new HashMap<String, Map<String, List<Integer>>>();
	}

	public void assignSplits() {
		// actualize variables
		List<String> computerPool = master.getComputerPool();

		// compute split Assignment
		int proportion = splitNb / computerPool.size(); 
		proportion = proportion*splitNb <= computerPool.size() ? proportion  : (proportion + 1);
		splitAssignment = new HashMap<Integer, String>();
		for(int splitID=0; splitID<splitNb; splitID++) {
			splitAssignment.put(splitID, computerPool.get(splitID / proportion));
		}

		// print assignment
		Utils.printAssignment(splitAssignment);
	}


	public void assignKeys(Map<Integer, List<String>> splitKeys) {
		// actualize variables
		List<String> computerPool = master.getComputerPool();
		actualizeSplitKeys(splitKeys);

		// compute key assignment to index from the keys returned by the map slaves
		indexKeys();

		// compute key assignment
		keyAssignment = new HashMap<String, String>();
		int proportion = keysID.size() / computerPool.size();
		proportion = proportion*keysID.size() <= computerPool.size() ? proportion  : (proportion + 1);
		int count = 0;
		for(Entry<String, Integer> e : keysID.entrySet()) {
			String key = e.getKey();
			keyAssignment.put(key, computerPool.get(count / proportion));
			count++;
		}

	}

	private void indexKeys() {
		keysID = new HashMap<String, Integer>();
		int index = 0;
		for(Map.Entry<Integer, List<String>> e : splitKeys.entrySet()) {
			for(String key : e.getValue()) {
				if(!keysID.containsKey(key)) {
					keysID.put(key, index++);
				}
			}
		}
	}

	/* ***************** *
	 * **** SETTERS **** *
	 * ***************** */

	public void setSplitNb(int splitNb) {
		this.splitNb = splitNb;
	}

	private void assignKeyToSplitID(String key, int splitID) {
		// modify secondary keys
		List<Integer> splitIDList;
		String splitComputerID = splitAssignment.get(splitID);
		Map<String, List<Integer>> computerIDTosplitIDList = keyTocomputerIDToSplitIDList.get(key);
		if(computerIDTosplitIDList == null) {
			computerIDTosplitIDList = new HashMap<String, List<Integer>>();
			splitIDList = new ArrayList<Integer>(); splitIDList.add(splitID);
			computerIDTosplitIDList.put(splitComputerID, splitIDList);
			keyTocomputerIDToSplitIDList.put(key, computerIDTosplitIDList);
		}
		else {
			splitIDList = computerIDTosplitIDList.get(splitComputerID);
			if(splitIDList == null) {
				splitIDList = new ArrayList<Integer>(); splitIDList.add(splitID);
				computerIDTosplitIDList.put(splitComputerID, splitIDList);
			} 
			else {
				splitIDList.add(splitID);
			}
		}
	}

	private void actualizeSplitKeys(Map<Integer, List<String>> splitKeys) {
		this.splitKeys = splitKeys;
		for(Entry<Integer, List<String>> e : splitKeys.entrySet()) {
			int splitID = e.getKey();
			List<String> keyList = e.getValue();
			for(String key : keyList) {
				assignKeyToSplitID(key, splitID);
			}
		}
	}

	/* ***************** *
	 * **** GETTERS **** *
	 * ***************** */

	public Map<Integer, String> getSplitAssignment() {
		return splitAssignment;
	}

	public Map<String, String> getKeyAssignment() {
		return keyAssignment;
	}

	public Map<String, Integer> getKeysID() {
		return keysID;
	}

	public Map<String, Map<String, List<Integer>>> getKeyTocomputerIDToSplitIDList() {
		return keyTocomputerIDToSplitIDList;
	}


}
