package slaveHandler;

import java.util.HashMap;
import java.util.Map;
import java.util.function.Consumer;

import master.Master;
import utils.Pair;

public class IdentifiedSlaveHandler<ID, R> extends DefaultSlaveHandler<Pair<ID, R>>{

	public IdentifiedSlaveHandler(Master master, Consumer<Map<ID, R>> notifyCompletionFunction) {
		super(master, createIdentifiedSlaveHandlerTask(notifyCompletionFunction));
	}
	
	public synchronized void notifySlaveCompletion(ID slaveID, R result) {
		super.notifySlaveCompletion(new Pair<ID,R>(slaveID, result));
	}
	
	static <ID, R> HandlerTask<Pair<ID, R>> createIdentifiedSlaveHandlerTask(Consumer<Map<ID, R>> notifyCompletionFunction) {
		HandlerTask<Pair<ID, R>> task = new HandlerTask<Pair<ID,R>>() {
			private final Map<ID, R> resultMap = new HashMap<ID, R>();
			
			@Override
			public void notifyResult(Pair<ID, R> result) {
				resultMap.put(result.getFirst(), result.getSecond());
			}
			
			@Override
			public void notifyCompletion() {
				notifyCompletionFunction.accept(resultMap);
			}
		}; 
		return task;
	}
}
