package taskGraph;

public interface GraphManager {
	public void notifyCompletion();
	public void notifyTaskFailure();
	public void notifyComputerCrash(String computerID);
	
}
