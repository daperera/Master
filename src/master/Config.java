package master;

public final class Config {
	
	// directory hierarchy
	public static final String COMPUTER_POOL_FILENAME = "data/computer_pool/computer_pool";
	public static final String SPLITTED_CHUNKS_FILEPATH = "data/splits/S";
	public final static String LOCAL_SPLIT_FOLDER = "data/splits";
	public final static String REMOTE_TEMP_FOLDER = "tmp";
	public final static String REMOTE_ROOT_FOLDER = "/tmp/dperera";
	public final static String REMOTE_SPLIT_FOLDER = "splits";
	public final static String REMOTE_MAP_FOLDER = "map";
	public static final String REMOTE_REDUCE_FOLDER = "reduce";
	public static final String REMOTE_REDUCE_TEMP_FOLDER = "reduce/tmp";
	public final static String SLAVE_FILENAME = "slave.jar";
	public final static String SPLIT_DEFAULT_FILENAME = "S";
	public final static String MAP_DEFAULT_FILENAME = "UM";
	public static final String REDUCE_DEFAULT_FILENAME = "RM";
	
	// algorithm default configuration
	public static final int DEFAULT_CHUNK_SIZE = 3;
	public static final String DEFAULT_INPUT_FILEPATH = "data/input/input";
	public static final int DEFAULT_CHECKED_COMPUTER_BATCH_SIZE = 10;
	public static final int DEFAULT_PARALLEL_COMPUTER_NUMBER = 10;
	public static final long DEFAULT_PROCESS_TIMEOUT = 10000;
	
}
