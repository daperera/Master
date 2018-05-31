package slaveHandler;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import master.Master;
import slave.SlaveTask;
import slave.SlaveThread;

public class DefaultSlaveHandler<R> implements SlaveHandler {
	
	protected int slavesCount;
	protected final List<SlaveTask> slavesTasks;
	protected final Master master;
	protected HandlerTask<R> handlerTask;
	
	public DefaultSlaveHandler(Master master, HandlerTask<R> task) {
		this.master = master;
		this.handlerTask = task;
		slavesCount = 0;
		slavesTasks = new ArrayList<SlaveTask>(); 
	}
	
	public synchronized void assignTask(SlaveTask slaveTask) {
		slavesTasks.add(slaveTask);
		slavesCount++;
	}

	public synchronized void notifySlaveCompletion(R result) {
		slavesCount--;
		handlerTask.notifyResult(result);
		testCompletionAndNotify();
	}
	
	private synchronized boolean testCompletionAndNotify() {
		if(slavesCount == 0) {
			handlerTask.notifyCompletion();
			return true;
		}
		return false;
	}
	
	public synchronized void notifySlaveCrash() {
		slavesCount--;
	}
	
	public void start() {
		if(testCompletionAndNotify()); // if no task assigned, notify completion
		else { // else create a new SlaveThread per task
			for(Iterator<SlaveTask> it = slavesTasks.iterator(); it.hasNext();) {
				SlaveTask slaveTask = it.next();
				it.remove();
				(new SlaveThread(slaveTask)).start();
			}
		}
	}
}
