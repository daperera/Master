package slave;

import java.util.concurrent.TimeUnit;

public class SlaveThread extends Thread {
	
	private final static long TIME_OUT = 10000;
	
	private SlaveTask task;
	
	public SlaveThread(SlaveTask task) {
		this.task = task;
	}
	
	@Override
	public void run() {
		Process p = task.execute();
		try {
			p.waitFor(TIME_OUT, TimeUnit.MILLISECONDS);
			int exitValue = p.exitValue();
			if(exitValue==0) { // if the task succeeded
				
				// wait for all messages to have been receives by the ThreadMessageListner, if any
				// WARNING : MOCK IMPLEMENTATION
				try {
					Thread.sleep(500);
				} catch (InterruptedException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
				// WARNING : END MOCK IMPLEMENTATION
				
				task.notifyCompletion();
			}
			else {
				notifyProcessCrash(p);
			}
		} catch (InterruptedException | IllegalThreadStateException e) {
//			e.printStackTrace();
			notifyProcessCrash(p);
		}
		
		
		
	}
	
	private void notifyProcessCrash(Process p) {
		task.notifyProcessCrash();
		p.destroy(); // try to kill the process		
		try {
			p.waitFor(TIME_OUT, TimeUnit.MILLISECONDS); // give it a chance to stop
		} catch (InterruptedException e) {
			e.printStackTrace();
		} 
		p.destroyForcibly(); // try to kill it harder
	}
	
	
	
}