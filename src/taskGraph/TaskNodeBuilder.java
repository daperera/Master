package taskGraph;

import java.util.List;

import taskGraph.resource.Resource;

public class TaskNodeBuilder {
	
	private final TaskNode node;
	private boolean hasPreviousNode;
	private Task task;
	private ProcesslessTask processlessTask;
	private ResultDeliverer deliverer;
	
	public static TaskNodeBuilder newBuilder() {
		return new TaskNodeBuilder();
	}
	
	public TaskNodeBuilder() {
		node = new TaskNode(null);
		hasPreviousNode = false;
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
	
	public TaskNode build() {
		// if deliverer not defined
		if(deliverer == null) {
			deliverer = new ResultDeliverer() {
				@Override
				public void deliverResult(TaskNode parentNode, List<String> messages) {
					return; // do nothing by default
				}};
		}
		
		// assign task
		if(task != null) {
			node.assignTask(task, deliverer);
		} else if(processlessTask != null) {
			node.assignTask(processlessTask, deliverer);
		}
		else {
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
