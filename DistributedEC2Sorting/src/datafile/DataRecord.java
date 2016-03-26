package datafile;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.io.Serializable;
import java.util.zip.GZIPInputStream;

import utils.FileSystem;

/*
 * We plan to identify each data entry as a record using the following:
 * - file name in which it is located
 * - character offset of from the beginning of the file to the start of record
 * - length of the record
 * - value in the record which will be used to sort the dataset : value of “Dry Bulb Temp” field 
 * */


public class DataRecord implements Serializable, Comparable<DataRecord>{

	private String fileName;
	private long fromChar;
	private int recordLength;
	private double sortValue;
	
	// CONSTRUCTORS
	
	public DataRecord(String fileName, long fromChar, int recordLength) {
		this.fileName = fileName;
		this.fromChar = fromChar;
		this.recordLength = recordLength;
	}
	
	public DataRecord(String fileName, long fromChar, int recordLength, double sortValue) {
		super();
		this.fileName = fileName;
		this.fromChar = fromChar;
		this.recordLength = recordLength;
		this.sortValue = sortValue;
	}

	// DataRecord Methods
	
	/*
	 * Read the complete record from the file system and return as a string..
	 * */
	public String readRecord(String bucketName){
		String lineToRead="";

		try {
			FileSystem myFs = new FileSystem();
			
			Reader decoder = myFs.getFileReader(bucketName, this.fileName);
			
			char[] cbuf = new char[1000];	
			decoder.skip(this.fromChar);
			decoder.read(cbuf, 0, this.recordLength);
			lineToRead = new String(cbuf);
			
			cbuf = null;			
			decoder.close();
			
		} catch (FileNotFoundException e) {
			System.err.println("Unable to locate file " + this.fileName);
			e.printStackTrace();
		} catch (IOException e) {
			System.err.println("Unable to read from file " + this.fileName);
			e.printStackTrace();
		}
		
		return lineToRead;
	}

	
	@Override
	public int compareTo(DataRecord o) {
		// TODO Auto-generated method stub
		return 0;
	}
	
	// GETTERS AND SETTERS
	
	public String getFileName() {
		return fileName;
	}
	
	public void setFileName(String fileName) {
		this.fileName = fileName;
	}
	
	public long getFromChar() {
		return fromChar;
	}
	
	public void setFromChar(long fromChar) {
		this.fromChar = fromChar;
	}

	public int getRecordLength() {
		return recordLength;
	}

	public void setRecordLength(int recordLength) {
		this.recordLength = recordLength;
	}

	public double getSortValue() {
		return sortValue;
	}

	public void setSortValue(double sortValue) {
		this.sortValue = sortValue;
	}
}
