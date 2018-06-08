package taskGraph.resource;

public abstract class Resource {
	private final String ID;
	
	public Resource(String ID) {
		this.ID = ID;
	}
	
	public String getID() {
		return ID;
	}
	
	public boolean equals(Resource other) {
		if(other==null)
			return false;
		return ID.equals(other.ID);
	}
	
	public abstract void visit(ResourceVisitor visitor);
	
}
