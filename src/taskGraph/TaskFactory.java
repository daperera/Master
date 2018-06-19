package taskGraph;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.math.BigInteger;

import slave.CommandLineAdapter;
import taskGraph.resource.ResourceFactory;
import utils.Config;
import utils.Utils;


public class TaskFactory {

	private final Master master;



	public TaskFactory(Master master) {
		this.master = master;
	}

	public void splitInput(String path) {
		TaskNodeBuilder.newBuilder()
		.setManager(master)
		.assignTask((listener) -> { 
			int nFile = Utils.divideFile(path, Config.DEFAULT_CHUNK_SIZE); 
			InputStream in = new ByteArrayInputStream(BigInteger.valueOf(nFile).toByteArray());			
			listener.setInputStream(in);
		})
		.assignResultDeliverer( (n, messages) -> {
			int nFile = Integer.parseInt(messages.get(0));
			for(int k=0; k<nFile; k++) {
				deploySplit(k);
			}
		})
		.build();
	}

	public void deploySplit(int splitID) {
		String computerID = master.getComputerPool().getAvailableComputer();

		// create node
		TaskNodeBuilder.newBuilder()
		.setManager(master)
		.addRequiredResource(ResourceFactory.endComputer(computerID))
		.addProducedResource(ResourceFactory.splitFile(splitID))
		.assignTask(() -> { return CommandLineAdapter.deploySplit(splitID, computerID);})
		.assignResultDeliverer((parentNode, m) -> {map(parentNode, computerID, splitID);})
		.build();

	}

	public void map(TaskNode parentNode, String computerID, int splitID) {
		TaskNode mapSyncNode = getMapSyncNode();

		TaskNodeBuilder.newBuilder()
		.setManager(master)
		.addRequiredResource(ResourceFactory.endComputer(computerID))
		.addRequiredResource(ResourceFactory.splitFile(splitID))
		.addProducedResource(ResourceFactory.mapFile(splitID))
		.assignPreviousNode(parentNode)
		.assignTask(() -> { return CommandLineAdapter.startMapSlave(splitID, computerID); })
		.assignResultDeliverer((n, messages) -> { master.deliverMapKey(splitID, messages); })
		.assignNextNode(mapSyncNode)
		.build();

	}

	private TaskNode getMapSyncNode() {
		return MapSyncNode.getMapSyncNode(master);
	}
	
	
	public void checkComputer(String computerID) {
		TaskNodeBuilder.newBuilder()
		.setManager(master)
		.addRequiredResource(ResourceFactory.endComputer(computerID))
		.setTimeOut(1000) // accelerate time out of process to check computer more rapidly
		.assignTask(() -> { return CommandLineAdapter.checkConnectivity(computerID);})
		.assignResultDeliverer(() -> {master.getComputerPool().setComputerAvailable(computerID);})
		.build();
	}
}
