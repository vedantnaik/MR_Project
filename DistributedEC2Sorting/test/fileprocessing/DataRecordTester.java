package fileprocessing;

import java.io.IOException;

import utils.S3FileReader;
import datafile.DataRecord;

public class DataRecordTester {

	public static void main(String[] args) {
		
		try {
			
			
			S3FileReader s3fr = new S3FileReader("cs6240sp16", "climate/199703hourly.txt.gz");
			
			
			
			
//			DataRecord dr1 = new DataRecord("climate/199703hourly.txt.gz", 473, 607 - 473 - 1, 26.1);
//			DataRecord dr2 = new DataRecord("climate/199703hourly.txt.gz", 743, 877 - 743 - 1, 25);
//			DataRecord dr3 = new DataRecord("climate/199703hourly.txt.gz", 1413, 1545 - 1413 - 1, 25);
//			
//			
//			
////			dr1.readRecordFrom(s3fr);
//			System.out.println(dr1.readRecordFrom(s3fr));
//			
//			
////			dr2.readRecordFrom(s3fr);
//			System.out.println(dr2.readRecordFrom(s3fr));
//			
//			System.out.println(dr3.readRecordFrom(s3fr));
//			
//			System.out.println(dr2.compareTo(dr1));
		
		
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		
		
		
		
		
		

	}
	
}
