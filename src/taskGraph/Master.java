package taskGraph;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class Master implements GraphManager {

	private final Map<Integer, List<String>> keyMap;
	private final ComputerPool computerPool;
	private final TaskFactory taskFactory;
	
	public Master() {
		taskFactory = new TaskFactory(this);
		computerPool = new ComputerPool(taskFactory);
		keyMap = new HashMap<Integer, List<String>>();
	}
	
	
	// MAP synchronisation point
	
	public void deliverMapKey(int splitID, List<String> messages) {
		// TODO Auto-generated method stub
		keyMap.put(splitID, messages);
	}

	public void notifyAllKeyCollected() {
		
		
		keyMap.clear();
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
