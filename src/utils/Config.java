package utils;

public final class Config {
	
	// directory hierarchy
	public static final String COMPUTER_POOL_FILENAME = "data/computer_pool/computer_pool";
	public static final String SPLITTED_CHUNKS_FILEPATH = "data/splits/S";
	public final static String LOCAL_SPLIT_FOLDER = "data/splits";
	public final static String LOCAL_SLAVE_FOLDER = "data/slave";
	public final static String REMOTE_TEMP_FOLDER = "tmp";  // ATTENTION : il est essentiel qu'il n'y ait pas de '/', 
															// sinon il s'agit d'un dossier local du serveur ssh
															// auquel on s'est connecté
															// (ordinateur ssh1 ou ssh2)
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
	public static final int DEFAULT_CHUNK_SIZE = 3; // chunk size for the file (in nb of words)
	public static final String DEFAULT_INPUT_FILEPATH = "data/input/input";
	public static final int DEFAULT_CHECKED_COMPUTER_BATCH_SIZE = 1; // number of computer checked simultaneously by the computerPool
	public static final int DEFAULT_PARALLEL_COMPUTER_NUMBER = 1;
	public static final long DEFAULT_PROCESS_TIMEOUT = 10000;
	public static final long DEFAULT_COMPUTER_REACHABILITY_CHECK_TIMEOUT = 1000;
	
}
