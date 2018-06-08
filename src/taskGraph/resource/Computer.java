package taskGraph.resource;

public class Computer extends Resource{

	public Computer(String ID) {
		super(ID);
	}
	
	@Override
	public void visit(ResourceVisitor visitor) {
		visitor.visitComputer(this);
	}
	
}
