package taskGraph;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import taskGraph.resource.Resource;

public class ResourceTracker {

	private final Map<Resource, Set<TaskNode>> requiredResources;
	private final Set<Resource> availableResources;

	public ResourceTracker() {
		requiredResources = new HashMap<Resource, Set<TaskNode>>();
		availableResources = new HashSet<Resource>();
	}

	public void notifyResourceAvailable(Resource r) {
		availableResources.add(r);
		if(requiredResources.get(r) != null) {
			for(TaskNode node : requiredResources.get(r)) {
				node.notifyResourceAvailable(r);
			}
		}
	}

	public void addRequiredResource(TaskNode node, Resource resource) {
		Set<TaskNode> dependantNodes = requiredResources.get(resource);
		if(dependantNodes==null) {
			dependantNodes = new HashSet<TaskNode>();
		}
		dependantNodes.add(node);
		requiredResources.put(resource, dependantNodes);
//		System.err.println("required resource added to tracker");
	}

	public boolean isAvailable(Resource r) {
		return availableResources.contains(r);
	}

}
