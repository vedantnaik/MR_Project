package datafile;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.Reader;
import java.io.Serializable;

import utils.S3FileReader;

/*
 * We plan to identify each data entry as a record using the following:
 * - file name in which it is located
 * - character offset of from the beginning of the file to the start of record
 * - length of the record
 * - value in the record which will be used to sort the dataset : value of Dry Bulb Temp field 
 * */

public class DataRecord implements Serializable, Comparable<DataRecord>{

	private static final long serialVersionUID = 1L;
//	private String fileName;
//	private long fromChar;
//	private int recordLength;
	private double sortValue;
	
	// DATA RECORD COMPLETE VALUES
	private String wban;
	private int date;
	private String time;
	
	// CONSTRUCTORS
//	public DataRecord(String fileName, long fromChar, int recordLength) {
//		this.fileName = fileName;
//		this.fromChar = fromChar;
//		this.recordLength = recordLength;
//	}
//	
//	public DataRecord(String fileName, long fromChar, int recordLength, double sortValue) {
//		this.fileName = fileName;
//		this.fromChar = fromChar;
//		this.recordLength = recordLength;
//		this.sortValue = sortValue;
//	}
	
	public DataRecord(DataRecord copyFrom) {
		this.sortValue = copyFrom.getSortValue();
		
		this.wban = copyFrom.getWban();
		this.date = copyFrom.getDate();
		this.time = copyFrom.getTime();
	}

	// DATA RECORD COMPLETE VALUES
	public DataRecord(double sortValue, String wban, int date, String time) {
//		this.fileName = fileName;
//		this.fromChar = fromChar;
//		this.recordLength = recordLength;
		this.sortValue = sortValue;
		
		this.wban = wban;
		this.date = date;
		this.time = time;
	}
	
	
	// DataRecord Methods
	
	/**
	 * Read the complete record from the file system and return as a string.
	 * */
//	public String readRecord(String bucketName){
//		String lineToRead="";
//
//		try {
//			S3FileReader myFs = new S3FileReader(bucketName, this.fileName);
//			
//			Reader decoder = myFs.getS3FileReader();
//			
//			char[] cbuf = new char[1000];	
//			decoder.skip(this.fromChar);
//			decoder.read(cbuf, 0, this.recordLength);
//			lineToRead = new String(cbuf);
//			
//			cbuf = null;			
//			decoder.close();
//			
//		} catch (FileNotFoundException e) {
//			System.err.println("Unable to locate file " + this.fileName);
//			e.printStackTrace();
//		} catch (IOException e) {
//			System.err.println("Unable to read from file " + this.fileName);
//			e.printStackTrace();
//		}
//		
//		return lineToRead;
//	}
//
//	public String readRecordFromS3(String inputBucketName) {
//		String[] fields = this.readRecord(inputBucketName).split(",");
//		String wban = DataFileParser.getValueOf(fields, DataFileParser.Field.WBAN_NUMBER);
//		String date = DataFileParser.getValueOf(fields, DataFileParser.Field.YEARMONTHDAY);
//		String time = DataFileParser.getValueOf(fields, DataFileParser.Field.TIME);
//		String dryBulbTemp = DataFileParser.getValueOf(fields, DataFileParser.Field.DRY_BULB_TEMP);
//		return wban+","+date+","+time+","+dryBulbTemp;
//	}
	
	// DATA RECORD COMPLETE VALUES
	/**
	 * Read the complete record from this object and return as a string.
	 * */
	public String readRecordFromObject(){
		return this.wban+","+this.date+","+this.time+","+this.sortValue;
	}

	
	/**
	 * Read the complete record from the file system and return as a string..
	 * */
//	public String readRecordFrom(S3FileReader s3fr) throws IOException{
//		return s3fr.readFromOffsetToLen(this.fromChar, this.recordLength);
//	}
	
	/**
	 * Display string on console. Used for debugging.
	 * */
	@Override
	public String toString() {
		return "<"+this.getSortValue()+">";
	}
	
	/**
	 * Compare two DataRecord objects using sort value field
	 * */
	@Override
	public int compareTo(DataRecord o) {
		if(this.sortValue > o.getSortValue()) {return 1;}
		if(this.sortValue < o.getSortValue()) {return -1;}
		return 0;
	}
	
	// GETTERS AND SETTERS
	
//	public String getFileName() {
//		return fileName;
//	}
//	
//	public void setFileName(String fileName) {
//		this.fileName = fileName;
//	}
//	
//	public long getFromChar() {
//		return fromChar;
//	}
//	
//	public void setFromChar(long fromChar) {
//		this.fromChar = fromChar;
//	}
//
//	public int getRecordLength() {
//		return recordLength;
//	}
//
//	public void setRecordLength(int recordLength) {
//		this.recordLength = recordLength;
//	}

	public double getSortValue() {
		return sortValue;
	}

	public void setSortValue(double sortValue) {
		this.sortValue = sortValue;
	}

	// DATA RECORD COMPLETE VALUES
	public String getWban() {
		return wban;
	}

	public void setWban(String wban) {
		this.wban = wban;
	}

	public int getDate() {
		return date;
	}

	public void setDate(int date) {
		this.date = date;
	}

	public String getTime() {
		return time;
	}

	public void setTime(String time) {
		this.time = time;
	}

	
	
}
