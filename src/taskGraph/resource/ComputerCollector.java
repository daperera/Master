package taskGraph.resource;

import java.util.HashSet;
import java.util.Set;

public class ComputerCollector implements ResourceVisitor {
	
	public Set<Computer> computers;
	
	public Set<Computer> collect(Set<Resource> resources) {
		computers = new HashSet<Computer>();
		for(Resource r : resources) {
			r.visit(this);
		}
		return computers;
	}
	
	@Override
	public void visitEndComputer(EndComputer computer) {
		computers.add(computer);
	}

	@Override
	public void visitComputer(Computer computer) {
		computers.add(computer);
	}
}
