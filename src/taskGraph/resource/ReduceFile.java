package taskGraph.resource;

public class ReduceFile extends File{
	
	
	public ReduceFile(int ID) {
		super(ID);
	}
	
	public ReduceFile(String ID) {
		super(ID);
	}
	
	@Override
	public void visit(ResourceVisitor visitor) {
		visitor.visitReduceFile(this);
	}
	
	@Override
	public boolean equals(Object other) {
		if(other==null)
			return false;
		if(other.getClass()!=ReduceFile.class)
			return false;
		return ID.equals(((ReduceFile) other).ID);
	}
	
	@Override
	public int hashCode() {
	    return 1;
	}
}
