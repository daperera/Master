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

	public default void visitComputer(Computer resource) {
		return; // do nothing
	}

	public default void visitReduceFile(ReduceFile reduceFile) {
		return;
	}

	public default void visitReduceTmpFile(ReduceTmpFile reduceTmpFile) {
		return;
	}

	public default void visitReduceTmp2File(ReduceTmp2File reduceTmp2File) {
		return;
	}
	
}
