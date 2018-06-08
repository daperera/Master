package taskGraph.resource;

import java.util.Set;

public class ResourceFactory {
	
	public static Resource splitFile(int splitID) {
		return new SplitFile(splitID);
	}

	public static Resource endComputer(String computerID) {
		return new EndComputer(computerID);
	}

	public static Resource mapFile(int mapID) {
		return new MapFile(mapID);
	}
	
	public static Set<Computer> collectComputer(Set<Resource> resources) {
		return new ComputerCollector().collect(resources);
	}
	
	
}
