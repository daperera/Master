package taskGraph.resource;

public class SplitFile extends File {

	public SplitFile(String ID) {
		super(ID);
	}
	
	public SplitFile(int ID) {
		super(ID);
	}
	
	@Override
	public void visit(ResourceVisitor visitor) {
		visitor.visitSplitFile(this);
	}
	
	@Override
	public boolean equals(Object other) {
		if(other==null)
			return false;
		if(other.getClass()!=SplitFile.class)
			return false;
		return ID.equals(((Resource) other).ID);
	}
	
	@Override
	public int hashCode() {
	    return 1;
	}
}
