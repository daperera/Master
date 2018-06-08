package taskGraph;

import java.util.Deque;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import utils.Config;
import utils.Utils;

public class ComputerPool {
	
	private TaskFactory taskFactory;
	
	private Map<String, Integer> reachableComputer; // computerID -> number of process using it
	private Deque<String> toCheckComputer; // computers that we are not sure we can reach
	
	public ComputerPool(TaskFactory taskFactory) {
		this.taskFactory = taskFactory;

		// build reachableComputer list
		reachableComputer = new HashMap<String, Integer>();
		
		// build availableComputer list
		List<String> availableComputer = Utils.defaultComputerPool();
		
		// build toCheckComputer queue
		toCheckComputer = new LinkedList<String>();
		for(String computer : availableComputer) {
			toCheckComputer.addFirst(computer);
		}
		
		// start checking computers
		for(int k=0; k<Config.DEFAULT_CHECKED_COMPUTER_BATCH_SIZE; k++) {
			checkNextComputer();
		}
		
	}
 	
	public String getAvailableComputer() {
		// get least used computer
		String minKey = Utils.min(reachableComputer);
		
		// increase number of process using it
		Integer value = reachableComputer.get(minKey);
		reachableComputer.put(minKey, value + 1); 
		return minKey;
	}
	
	public void notifyComputerCrash(String computerID) {
		System.out.println("computer crashed : " + computerID);
		
		reachableComputer.remove(computerID);
		toCheckComputer.addLast(computerID);
	}

	public void setComputerAvailable(String computerID) {
		System.err.println("computer available : " + computerID);
		
		// add computerID to the list of available computers
		reachableComputer.put(computerID, 0);
		
		// check next computer
		checkNextComputer();
	}
	
	private void checkNextComputer() {
		if(!toCheckComputer.isEmpty()) {
			String computerId = toCheckComputer.getFirst();
			
			System.err.println("checking computer : " + computerId);
	
			taskFactory.checkComputer(computerId);
			toCheckComputer.remove(computerId);
		}
	}
}
