package fileprocessing;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;

import datafile.DataFileParser;
import datafile.DataRecord;
import utils.FileSystem;
import utils.S3FileReader;

public class DataFileParserTester {

	
	public static void main(String[] args) throws IOException {
		
		S3FileReader s3fr = new S3FileReader("cs6240sp16", "climate/199703hourly.txt.gz");
		ArrayList<DataRecord> drListFor199703 = FileSystem.readRecordsFrom("cs6240sp16", "climate/199703hourly.txt.gz");
		
		System.out.println("make it sort");
		
		Collections.sort(drListFor199703);
		
		for (DataRecord dr : drListFor199703){
			System.out.println(dr.getSortValue() + " ");
			// System.out.println(dr.readRecordFrom(s3fr));
		}
		
	}
	
}
