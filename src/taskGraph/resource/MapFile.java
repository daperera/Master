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
	
}
