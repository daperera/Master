package utils;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * This class provide utility methods
 *
 */

public class Utils {
	
	
	public static List<String> defaultComputerPool() {
		// read computer names in dedicated file
		List<String> computerPool = new ArrayList<String>();
		WordReader reader = new WordReader(Config.COMPUTER_POOL_FILENAME);
		while(reader.hasNext()) { // for each computer name
			String computerName = reader.next();
			computerPool.add(computerName);
		}
		return computerPool;
	}
	
	public static void printAssignment(Map<Integer, String> splitAssignment) {
		for(Map.Entry<Integer, String> e : splitAssignment.entrySet()) {
			int splitID = e.getKey();
			String computerID = e.getValue();
			System.out.println("split " + splitID + " : " + computerID);
		}
	}
	
	public static void printKeys(Map<Integer, List<String>> keyMap) {
		for(Map.Entry<Integer, List<String>> e : keyMap.entrySet()) {
			int mapID = e.getKey();
			List<String> keys = e.getValue();
			System.out.print(keys.size() + " keys found by process " + mapID + " :");
			for(String key : keys) {
				System.out.print(" " + key);
			}
			System.out.println("");
		}
	}
	
	public static void printInvertedMapping(Map<String, Map<String, List<Integer>>> invertedMapping) {
		System.out.println("Printing inverted map");
		for(Map.Entry<String, Map<String, List<Integer>>> e1 : invertedMapping.entrySet()) {
			String k1 = e1.getKey();
			Map<String, List<Integer>> v1 = e1.getValue();
			for(Map.Entry<String, List<Integer>> e2 : v1.entrySet()) {
				String k2 = e2.getKey();
				List<Integer> v2 = e2.getValue();
				System.out.print(k1 + " -> " + k2 + " ->");
				for(int v3 : v2) {
					System.out.println(" " + v3);
				}
			}
		}
		System.out.println("End print");
	}

	public static void printKeyID(Map<String, Integer> keyID) {
		System.out.println("printing keyID");
		for(Map.Entry<String, Integer> e : keyID.entrySet()) {
			String k = e.getKey();
			int v = e.getValue();
			System.out.println(k + " -> " + v);
		}
		System.out.println("End print");
	}

	public static void printReducedValues(Map<String, Integer> reducedValues) {
		System.out.println("Algorithm result");
		for(Map.Entry<String, Integer> e : reducedValues.entrySet()) {
			String key = e.getKey();
			int value = e.getValue();
			System.out.println(key + " -> " + value);
		}
	}

	public static String getKeyFromValue(Map<String, Integer> keysID, int iD) {
		String key = null;
		for(Map.Entry<String, Integer> e : keysID.entrySet()) {
			if(e.getValue() == iD)
				key = e.getKey();
		}
		return key;
	}

	public static List<String> extractAvailableComputerList(Map<String, Boolean> computerAvailability) {
		List<String> computerPool = new ArrayList<String>();
		for(Map.Entry<String, Boolean> e : computerAvailability.entrySet()) {
			String computerID = e.getKey();
			boolean available = e.getValue();
			if(available)
				computerPool.add(computerID);
		}
		return computerPool;
	}

	public static int divideFile(String inputFilepath, int chunkSize) {
		WordReader reader = new WordReader(inputFilepath);
		List<String> buffer = new ArrayList<String>();
		int chunkIndex = 0;
		while(reader.hasNext()) {
			buffer.add(reader.next());
			if(buffer.size() >= chunkSize) {
				String chunkFilename = Config.SPLITTED_CHUNKS_FILEPATH + chunkIndex; 
				writeChunk(buffer, chunkFilename);
				chunkIndex++;
				buffer.clear();
			}
		}
		return chunkIndex;
	}

	private static void writeChunk(List<String> buffer, String filename) {
		BufferedWriter writer = null;
		try {
			writer = new BufferedWriter(new FileWriter(filename));
			for(String word : buffer) {
				writer.write(word + " ");
			}
		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			if(writer != null) {
				try {
					writer.close();
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
		}
	}
	
	
	
}
