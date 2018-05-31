package slave;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.List;

public class ThreadMessageListener extends Thread {
	List<String> messages;
	BufferedReader reader;
	
	public ThreadMessageListener(List<String> messageQueue, InputStream in) {
		this.messages = messageQueue;
		this.reader = new BufferedReader(new InputStreamReader(in));
	}

	@Override
	public void run() {
		while(true) {
			String message;
			try {
				message = reader.readLine();
				if(message != null) {
					messages.add(message);
//					System.out.println("message : " + message);
				}
				Thread.sleep(200);
			} catch (IOException | InterruptedException e1) {
				e1.printStackTrace();
			}

		}
	}

}
