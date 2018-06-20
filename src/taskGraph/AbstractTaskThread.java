package taskGraph;

public abstract class AbstractTaskThread extends Thread {
	
	protected final TaskNode manager;
	protected ResultDeliverer deliverer;
	private ThreadMessageListener listener;
	
	
	
	public AbstractTaskThread(TaskNode taskNode, ResultDeliverer deliverer) {
		this.manager = taskNode;
		this.deliverer = deliverer;
		
	}

	public abstract void startTask(ThreadMessageListener listener);
	
	@Override
	public final void run() {
		listener = new ThreadMessageListener();
		listener.start();
		startTask(listener);
	}
	
}
