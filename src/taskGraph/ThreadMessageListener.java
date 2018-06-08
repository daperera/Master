package taskGraph;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;

public class ThreadMessageListener extends Thread {
	List<String> messages;
	BufferedReader reader;
	
	public ThreadMessageListener() {
		messages = new ArrayList<String>();
		reader = null;
	}

	public void setInputStream(InputStream in) {
		reader = new BufferedReader(new InputStreamReader(in));
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
	
	public List<String> getMessages() {
		return messages;
	}

}
