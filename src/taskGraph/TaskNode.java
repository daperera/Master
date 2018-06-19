package taskGraph;

import java.util.HashSet;
import java.util.Set;

import taskGraph.resource.Computer;
import taskGraph.resource.Resource;
import taskGraph.resource.ResourceFactory;

public class TaskNode implements TaskNodeInterface {
	
	// graph related parameters
	private final Set<TaskNodeInterface> previousNodes;
	private final Set<TaskNodeInterface> nextNodes;
	private final Set<Resource> requiredResources;
	private final Set<Resource> producedResources;
	private final Set<TaskNodeInterface> successfullPreviousNodes;
	
	// task related parameters
	private GraphManager manager;
	private AbstractTaskThread taskThread;
	private boolean taskCompleted; 
	
	public TaskNode(GraphManager manager) {
		this.manager = manager;
		taskCompleted = false;
		
		previousNodes = new HashSet<TaskNodeInterface>();
		nextNodes = new HashSet<TaskNodeInterface>();
		requiredResources = new HashSet<Resource>();
		producedResources = new HashSet<Resource>();
		successfullPreviousNodes = new HashSet<TaskNodeInterface>();
	}
	
	@Override
	public void assignTask(Task task, ResultDeliverer deliverer, long timeOut) {
		taskThread = new TaskThread(this, task, deliverer, timeOut);
	}
	
	@Override
	public void assignTask(Task task, ResultDeliverer deliverer) {
		taskThread = new TaskThread(this, task, deliverer);
	}
	
	@Override
	public void assignTask(ProcesslessTask task, ResultDeliverer deliverer) {
		taskThread = new ProcesslessTaskThread(this, task, deliverer);
	}
	
	@Override
	public void assignPreviousNode(TaskNodeInterface node) {
		previousNodes.add(node);
		if(node.taskCompleted()) {
			successfullPreviousNodes.add(node);
		}
	}

	@Override
	public void assignNextNode(TaskNodeInterface node) {
		nextNodes.add(node);
	}
	
	public void addRequiredResource(Resource resource) {
		requiredResources.add(resource);
	}
	
	public void addProducedResource(Resource resource) {
		producedResources.add(resource);
	}

	@Override
	public void notifyPreviousNodeReady(TaskNodeInterface taskNode) {
		if(previousNodes.contains(taskNode)) { // if task creates required ressources
			successfullPreviousNodes.add(taskNode);
			if(successfullPreviousNodes.size() == previousNodes.size()) // if all required resources have become available, and task not yet completed
				taskThread.start(); // start task
		}
	}
	
	@Override
	public void notifyPreviousNodeFailed(TaskNodeInterface taskNode) {
		successfullPreviousNodes.remove(taskNode);
	}
	
	public void forceStart() {
		taskThread.start();
	}

	public void setGraphManager(GraphManager manager) {
		this.manager = manager;
	}
	
	public void notifyComputerCrash() {
		Set<Computer> crashedComputers = ResourceFactory.collectComputer(requiredResources);
		for(Computer computer : crashedComputers) {
			String computerID = computer.getID();
			manager.notifyComputerCrash(computerID);
			
		}
	}
	
	public void notifyFailure() {
		manager.notifyTaskFailure();
	}
	
	public void notifyCompletion() {
		taskCompleted = true;
		for(TaskNodeInterface node : nextNodes) {
			node.notifyPreviousNodeReady(this);
		}
	}
	
	public boolean taskCompleted() {
		return taskCompleted;
	}

}	
