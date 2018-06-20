package taskGraph.resource;

public abstract class Resource {
	protected final String ID;
	
	public Resource(String ID) {
		this.ID = ID;
	}
	
	public String getID() {
		return ID;
	}
	
	@Override
	public boolean equals(Object other) {
		if(other==null)
			return false;
		return ID.equals(((Resource) other).ID);
	}
	
	@Override
	public int hashCode() {
	    return 1;
	}
	
	public abstract void visit(ResourceVisitor visitor);
	
}
