package slave;

public interface SlaveTask {
	public  Process execute();
	public void notifyCompletion();
	public void notifyProcessCrash();
	public void notifyComputerCrash();
}
