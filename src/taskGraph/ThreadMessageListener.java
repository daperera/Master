package taskGraph;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;

public class ThreadMessageListener extends Thread {
	private volatile List<String> messages;
	private BufferedReader reader;

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
			try {
				readMessage();
				Thread.sleep(20);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}

		}
	}

	private void readMessage() {
		try {
			if(reader != null) {
				String message = reader.readLine();
				if(message != null) {
					messages.add(message);
//					System.out.println("message : " + message);
				}
			}
		} catch(IOException e) {
			e.printStackTrace();
		}
	}

	public List<String> getMessages() {
		readMessage(); // forcing message Read
		return messages;
	}

}
