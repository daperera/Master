package taskGraph;

import java.util.concurrent.TimeUnit;

public class TaskThread extends AbstractTaskThread {
	
	private final static long TIME_OUT = 10000;
	private final Task task;
	private final ThreadMessageListener errorListener;
	
	
	public TaskThread(TaskNode taskNode, Task task, ResultDeliverer deliverer) {
		super(taskNode, deliverer);
		this.task = task;
		errorListener = new ThreadMessageListener();
	}

	@Override
	public void startTask(ThreadMessageListener listener) {
		Process p = task.start();
		listener.setInputStream(p.getInputStream());
		errorListener.setInputStream(p.getErrorStream());
		
		try {
			p.waitFor(TIME_OUT, TimeUnit.MILLISECONDS);
			int exitValue = p.exitValue();
			if(exitValue==0) { // if the task succeeded
				
				// wait for all messages to have been receives by the ThreadMessageListner, if any
				// WARNING : MOCK IMPLEMENTATION
				try {
					Thread.sleep(500);
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
				// WARNING : END MOCK IMPLEMENTATION
				
				deliverer.deliverResult(manager, listener.getMessages());
			}
			else {
				notifyProcessCrash(p);
			}
		} catch (InterruptedException | IllegalThreadStateException e) {
//			e.printStackTrace();
			notifyProcessCrash(p);
		}
	}
	
	private void notifyProcessCrash(Process p) {
		// decide between computer failure ofr process failure
		if(isComputerFailure()) {
			manager.notifyComputerCrash();
		}
		else {
			manager.notifyFailure(); // notify failure to manager taskNode
		}
		p.destroy(); // try to kill the process		
		try {
			p.waitFor(TIME_OUT, TimeUnit.MILLISECONDS); // give it a chance to stop
		} catch (InterruptedException e) {
			e.printStackTrace();
		} 
		p.destroyForcibly(); // try to kill it harder
	}

	private boolean isComputerFailure() {
		return true;
	}

}
