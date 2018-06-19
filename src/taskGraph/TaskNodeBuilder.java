package taskGraph;

import java.util.List;

import taskGraph.resource.Resource;

public class TaskNodeBuilder {
	
	private final TaskNode node;
	private boolean hasPreviousNode;
	private Task task;
	private ProcesslessTask processlessTask;
	private ResultDeliverer deliverer;
	private long timeOut;
	
	public static TaskNodeBuilder newBuilder() {
		return new TaskNodeBuilder();
	}
	
	public TaskNodeBuilder() {
		node = new TaskNode(null);
		hasPreviousNode = false;
		timeOut = 0;
	}
	
	public TaskNodeBuilder setManager(GraphManager manager) {
		node.setGraphManager(manager);
		return this;
	}
	
	public TaskNodeBuilder assignTask(Task task) {
		this.task = task;
		return this;
	}
	
	public TaskNodeBuilder assignTask(ProcesslessTask task) {
		this.processlessTask = task;
		return this;
	}
	
	
	public TaskNodeBuilder assignResultDeliverer(ResultDeliverer deliverer) {
		this.deliverer = deliverer;
		return this;
	}
	
	public TaskNodeBuilder assignResultDeliverer(TaskNodeConsumer deliverer) {
		assignResultDeliverer(node ->  {deliverer.accept(node);});
		return this;
	}
	
	public TaskNodeBuilder assignResultDeliverer(Runnable deliverer) {
		assignResultDeliverer((n, m) ->  {deliverer.run();});
		return this;
	}
	
	
	
	public TaskNodeBuilder assignPreviousNode(TaskNodeInterface node) {
		this.node.assignPreviousNode(node);
		hasPreviousNode = true;
		return this;
	}

	public TaskNodeBuilder assignNextNode(TaskNodeInterface node) {
		this.node.assignNextNode(node);
		return this;
	}
	
	public TaskNodeBuilder addRequiredResource(Resource resource) {
		node.addRequiredResource(resource);
		return this;
	}
	
	public TaskNodeBuilder addProducedResource(Resource resource) {
		node.addProducedResource(resource);
		return this;
	}
	
	public TaskNodeBuilder setTimeOut(long timeOut) {
		this.timeOut = timeOut;
		return this;
	}
	
	public TaskNode build() {
		// if no deliverer has been set
		if(deliverer == null) {
			deliverer = new ResultDeliverer() {
				@Override
				public void deliverResult(TaskNode parentNode, List<String> messages) {
					return; // do nothing by default
				}};
		}
		
		// assign task
		if(task != null) {
			if(timeOut<=0) { // if no timeOut set or erroneous timeOut set
				node.assignTask(task, deliverer);
			} else {
				node.assignTask(task, deliverer, timeOut);
			}
		} else if(processlessTask != null) {
			node.assignTask(processlessTask, deliverer);
		}
		else { // if no task has been set
			node.assignTask((l)->{return;}, deliverer);
		}
		
		// start task
		if(!hasPreviousNode)
			node.forceStart();
		return node;
	}
	
	public interface TaskNodeConsumer {
		public void accept(TaskNode node);
	}
	
	
	
}
