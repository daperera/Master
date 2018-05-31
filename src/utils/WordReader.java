package utils;
import java.io.File;
import java.io.FileNotFoundException;
import java.util.Scanner;


/**
 * This class reads a file by words.
 * It exports three methods.
 * - hasNext() / next() : hasNext() returns true iff the file 
 * 	                      still contains a word, and next() 
 *                        returns this word or null if the end 
 *                        of the file has been reached.
 * - read() : alias for next().
 * @author dapinator
 */

public class WordReader {
		private Scanner lineScanner;
		private Scanner wordScanner;
		
		public WordReader(String fileName) {
			try {
				lineScanner = new Scanner(new File(fileName));
				if(lineScanner.hasNext()) {
					wordScanner = new Scanner(lineScanner.next());
				}
			} catch (FileNotFoundException e) {
				e.printStackTrace();
			}
		}
		
		public void close() {
			if(lineScanner != null)
				lineScanner.close();
			if(wordScanner != null)
				wordScanner.close();
		}
		
		
		public boolean hasNext() {
			if(wordScanner == null)
				return false;
			if(wordScanner.hasNext())
				return true;
			if(lineScanner.hasNext()) {
			    wordScanner = new Scanner(lineScanner.next());
			    return wordScanner.hasNext();
			}
			return false;
		}
		
		public String next() {
			if(!hasNext())
				return null;
			return wordScanner.next();
		}
		
		public String read() {
			return next();
		}
		
	}
