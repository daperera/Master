package taskGraph.resource;

public interface ResourceVisitor {

	public default void visitFile(File resource) {
		return; // do nothing
	}

	public default void visitSplitFile(SplitFile resource) {
		return; // do nothing
	}

	public default void visitMapFile(MapFile resource) {
		return; // do nothing
	}

	public default void visitEndComputer(EndComputer resource) {
		return; // do nothing
	}

	public default void visitComputer(Computer resource) {
		return; // do nothing
	}
	
}