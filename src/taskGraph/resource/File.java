package taskGraph.resource;

public class File extends Resource {

	public File(String ID) {
		super(ID);
	}

	public File(int ID) {
		super(String.valueOf(ID));
	}
	
	@Override
	public void visit(ResourceVisitor visitor) {
		visitor.visitFile(this);
	}

}
