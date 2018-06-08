package taskGraph;

public class ProcesslessTaskThread extends AbstractTaskThread {
	
	private ProcesslessTask task;
	
	public ProcesslessTaskThread(TaskNode taskNode, ProcesslessTask task, ResultDeliverer deliverer) {
		super(taskNode, deliverer);
		this.task = task;
	}

	@Override
	public void startTask(ThreadMessageListener listener) {
		task.start(listener);
	}
}
