package taskGraph.resource;

public class MapFile extends File {

	public MapFile(String ID) {
		super(ID);
	}
	
	public MapFile(int ID) {
		super(ID);
	}

	@Override
	public void visit(ResourceVisitor visitor) {
		visitor.visitMapFile(this);
	}
	
	@Override
	public boolean equals(Object other) {
		if(other==null)
			return false;
		if(other.getClass()!=MapFile.class)
			return false;
		return ID.equals(((Resource) other).ID);
	}
	
	@Override
	public int hashCode() {
	    return 1;
	}
	
}
