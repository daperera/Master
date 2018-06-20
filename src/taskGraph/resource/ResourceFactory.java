package taskGraph.resource;

import java.util.Set;

public class ResourceFactory {
	
	public static Resource splitFile(int splitID) {
		return new SplitFile(splitID);
	}

	public static Resource computer(String computerID) {
		return new Computer(computerID);
	}

	public static Resource mapFile(int mapID) {
		return new MapFile(mapID);
	}
	
	public static Set<Computer> collectComputer(Set<Resource> resources) {
		return new ComputerCollector().collect(resources);
	}

	public static Resource reduceTmpFile(int reduceID, String computerID) {
		return new ReduceTmpFile(reduceID, computerID);
	}
	
	public static Resource reduceFile(int reduceID) {
		return new ReduceFile(reduceID);
	}
	
	
}
