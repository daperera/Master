package taskGraph.resource;

public class ReduceTmpFile extends File{

	private String computerID;
	
	public ReduceTmpFile(int ID, String computerID) {
		super(ID);
		this.computerID = computerID;
	}
	
	public ReduceTmpFile(String ID, String computerID) {
		super(ID);
		this.computerID = computerID;
	}
	
	@Override
	public void visit(ResourceVisitor visitor) {
		visitor.visitReduceTmpFile(this);
	}
	
	@Override
	public boolean equals(Object other) {
		if(other==null)
			return false;
		if(other.getClass()!=ReduceTmpFile.class)
			return false;
		return ID.equals(((ReduceTmpFile) other).ID) && computerID.equals(((ReduceTmpFile) other).computerID);
	}
	
	@Override
	public int hashCode() {
	    return 1;
	}

}
