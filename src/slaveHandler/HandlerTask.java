package slaveHandler;


public interface HandlerTask<R> {
	public void notifyCompletion();
	public void notifyResult(R result);
}
