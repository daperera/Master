package taskGraph.resource;

public class ReduceTmp2File extends File {
	private String computerID;
	
	public ReduceTmp2File(int ID, String computerID) {
		super(ID);
		this.computerID = computerID;
	}
	
	public ReduceTmp2File(String ID, String computerID) {
		super(ID);
		this.computerID = computerID;
	}
	
	@Override
	public void visit(ResourceVisitor visitor) {
		visitor.visitReduceTmp2File(this);
	}
	
	@Override
	public boolean equals(Object other) {
		if(other==null)
			return false;
		if(other.getClass()!=ReduceTmp2File.class)
			return false;
		return ID.equals(((ReduceTmp2File) other).ID) && computerID.equals(((ReduceTmp2File) other).computerID);
	}
	
	@Override
	public int hashCode() {
	    return 1;
	}
}
