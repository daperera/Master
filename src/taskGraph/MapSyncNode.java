package taskGraph;

public class MapSyncNode extends TaskNode {

	private static MapSyncNode myself = null;
	
	public synchronized static MapSyncNode getMapSyncNode(Master master) {
		if(myself == null) {
			myself = new MapSyncNode(master);
		}
		return myself;
	}
	
	private MapSyncNode(Master master) {
		super(master);
		super.assignTask((l) -> {return;}, (n,m) -> {master.notifyAllKeyCollected();});
	}
	
}
