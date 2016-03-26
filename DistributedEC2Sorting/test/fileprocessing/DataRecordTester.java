package fileprocessing;

import datafile.DataRecord;
import utils.FileSystem;

public class DataRecordTester {

	public static void main(String[] args) {
		
		DataRecord dr1 = new DataRecord("climate/199703hourly.txt.gz", 473, 607 - 473 - 1, 26.1);
		
		dr1.readRecord("cs6240sp16");
		System.out.println(dr1.readRecord("cs6240sp16"));
		
		DataRecord dr2 = new DataRecord("climate/199703hourly.txt.gz", 743, 877 - 743 - 1, 25);
		// 			start + 2, nextStart - start

		dr2.readRecord("cs6240sp16");
		System.out.println(dr2.readRecord("cs6240sp16"));
		
		System.out.println(dr2.compareTo(dr1));

	}
	
}
