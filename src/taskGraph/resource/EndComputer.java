package taskGraph.resource;

public class EndComputer extends Computer{

	public EndComputer(String ID) {
		super(ID);
	}

	@Override
	public void visit(ResourceVisitor visitor) {
		visitor.visitEndComputer(this);
	}
}
