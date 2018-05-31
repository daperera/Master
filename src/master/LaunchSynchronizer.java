package master;


/**
 * The purpose of this class is to wait for all condition are respected
 * before launching the computation on the Master class.
 * These conditions are :
 * - the computation has been requested (the compute() method has been called)
 * - the connectivity check ended
 *
 */
public class LaunchSynchronizer {
	
	private boolean computationRequested;
	private boolean connectivityCheckEnded;
	
	public LaunchSynchronizer(Master master) {
		computationRequested = false;
		connectivityCheckEnded = false;
		
		// thread verifiant v�rifiant r�guli�rement que toutes
		// les conditions pour d�marrer la calcul sont respect�es
		// et le d�marrant une fois qu'elles le sont
		new Thread() {
			@Override
			public void run() {
				while(true) {
					if(computationRequested && connectivityCheckEnded) { // si les conditions sont respect�es
						master.notifyStartSplit(); // lancer le calcul
						return;
					}
					try {
						Thread.sleep(1000);
					} catch (InterruptedException e) {
						e.printStackTrace();
					}
				}
			}
		}.start();
	}
	
	public void notifyConnectivityCheckEnded() {
		computationRequested = true;
	}

	public void requestComputation() {
		connectivityCheckEnded = true;
	}


}
