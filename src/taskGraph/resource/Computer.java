package taskGraph.resource;

public class Computer extends Resource {

	public Computer(String ID) {
		super(ID);
	}
	
	@Override
	public void visit(ResourceVisitor visitor) {
		visitor.visitComputer(this);
	}
	
	@Override
	public boolean equals(Object other) {
		if(other==null)
			return false;
		if(other.getClass()!=Computer.class)
			return false;
		return ID.equals(((Resource) other).ID);
	}
	
	@Override
	public int hashCode() {
	    return 1;
	}

}
