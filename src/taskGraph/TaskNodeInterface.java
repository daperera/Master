package taskGraph;

public interface TaskNodeInterface {
	public void assignPreviousNode(TaskNodeInterface node);
	public void assignNextNode(TaskNodeInterface node);
	public void notifyPreviousNodeReady(TaskNodeInterface taskNode);
	public void notifyPreviousNodeFailed(TaskNodeInterface taskNode);
	public void assignTask(Task task, ResultDeliverer deliverer);
	public boolean taskCompleted();
	void assignTask(ProcesslessTask task, ResultDeliverer deliverer);
}
