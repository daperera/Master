package slaveHandler;

import master.Master;

public class ResultlessSlaveHandler extends DefaultSlaveHandler<Boolean>{

	public ResultlessSlaveHandler(Master master, Runnable notifyCompletionFunction) {
		super(master, createReturnlessSlaveHandler(master, notifyCompletionFunction));
	}
	
	public synchronized void notifySlaveCompletion() {
		super.notifySlaveCompletion(true);
	}
	
	private static HandlerTask<Boolean> createReturnlessSlaveHandler(Master master, Runnable notifyCompletionFunction) {
		HandlerTask<Boolean> task = new HandlerTask<Boolean>() {
			@Override
			public void notifyCompletion() {
				notifyCompletionFunction.run();
			}
			@Override
			public void notifyResult(Boolean result) {
				// do nothing
			}
		};
		return task;
	}
}
