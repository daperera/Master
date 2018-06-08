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
	
}
