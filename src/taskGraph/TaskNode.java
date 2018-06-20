package taskGraph;

import java.util.HashSet;
import java.util.Set;

import taskGraph.resource.Computer;
import taskGraph.resource.Resource;
import taskGraph.resource.ResourceFactory;

public class TaskNode implements TaskNodeInterface {

	// graph related parameters
	//	private final Set<TaskNodeInterface> previousNodes;
	//	private final Set<TaskNodeInterface> nextNodes;
	private final Set<Resource> requiredResources;
	private final Set<Resource> producedResources;
	//	private final Set<TaskNodeInterface> successfullPreviousNodes;
	private final Set<Resource> availableResources;
	private ResourceTracker tracker;

	// task related parameters
	private GraphManager manager;
	private AbstractTaskThread taskThread;
	private boolean taskCompleted; 

	public TaskNode(GraphManager manager, ResourceTracker tracker) {
		this.manager = manager;
		this.tracker = tracker;
		taskCompleted = false;

		//		previousNodes = new HashSet<TaskNodeInterface>();
		//		nextNodes = new HashSet<TaskNodeInterface>();
		requiredResources = new HashSet<Resource>();
		producedResources = new HashSet<Resource>();
		//		successfullPreviousNodes = new HashSet<TaskNodeInterface>();
		availableResources = new HashSet<Resource>();
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
	/*
	@Override
	public void assignPreviousNode(TaskNodeInterface node) {
		previousNodes.add(node);
		if(node.taskCompleted()) {
			successfullPreviousNodes.add(node);
		}
		node.assignNextNode(this);
	}

	@Override
	public void assignNextNode(TaskNodeInterface node) {
		nextNodes.add(node);
	}
	 */

	public void addRequiredResource(Resource resource) {
		requiredResources.add(resource);
	}
	/*
	public void addRequiredResource(Computer computer) {
		requiredResources.add(computer);
		availableResources.add(computer);
	}
	*/

	public void addProducedResource(Resource resource) {
		producedResources.add(resource);
	}
	/*
	@Override
	public void notifyPreviousNodeReady(TaskNodeInterface taskNode) {
		if(previousNodes.contains(taskNode)) { // if task creates required ressources
			successfullPreviousNodes.add(taskNode);
		}
	}

	@Override
	public void notifyPreviousNodeFailed(TaskNodeInterface taskNode) {
		successfullPreviousNodes.remove(taskNode);
	}
	 */	
	@Override
	public void notifyResourceAvailable(Resource resource) {
//		System.err.println("resource available notification");
		if(requiredResources.contains(resource)) {
//			System.err.println("available Resource found : " + (requiredResources.size()-availableResources.size()) + " left");
			availableResources.add(resource);
			if(availableResources.size()==requiredResources.size()) {
				taskThread.start();
			} else {
//				System.err.println("not starting since " + availableResources.size() + " < " + requiredResources.size());
			}
		}
		
	}

	public void forceStart() {
		if(requiredResources.isEmpty()) {
//			System.err.println("requiredResources is empty");
			taskThread.start();
			return;
		}
//		System.err.println("requiredResources is not empty");
		for(Resource r : requiredResources) {
			if(tracker.isAvailable(r)) {
				notifyResourceAvailable(r);
			}
		}
	}

	public void setGraphManager(GraphManager manager) {
		this.manager = manager;
	}

	public void setResourceTracker(ResourceTracker tracker) {
		this.tracker = tracker;
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
		for(Resource r :producedResources) {
			tracker.notifyResourceAvailable(r);
		}
	}

	public boolean taskCompleted() {
		return taskCompleted;
	}

}	
