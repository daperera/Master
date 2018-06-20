package taskGraph;

import taskGraph.resource.Resource;

public interface TaskNodeInterface {
//	public void assignPreviousNode(TaskNodeInterface node);
//	public void assignNextNode(TaskNodeInterface node);
//	public void notifyPreviousNodeReady(TaskNodeInterface taskNode);
//	public void notifyPreviousNodeFailed(TaskNodeInterface taskNode);
	public void assignTask(Task task, ResultDeliverer deliverer);
	void notifyResourceAvailable(Resource resource);
	public boolean taskCompleted();
	void assignTask(ProcesslessTask task, ResultDeliverer deliverer);
	void assignTask(Task task, ResultDeliverer deliverer, long timeOut);
	
}
