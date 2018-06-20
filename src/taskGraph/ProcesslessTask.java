package taskGraph;

public interface ProcesslessTask  {
	
	public void start(ThreadMessageListener listener) throws Exception;
	
}
