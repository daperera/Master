package taskGraph;

import java.util.List;

public interface ResultDeliverer {
	public void deliverResult(TaskNode parentNode, List<String> messages);
}
