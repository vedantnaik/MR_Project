package fileprocessing;

import java.util.ArrayList;
import java.util.List;

public class Pivoting {

	public static void main(String[] args) {
		
		List<Double> serverDataRecordPivotValuesList = new ArrayList<>();
		String test = "-4.0, 12.9, 15.1, 16.0, 24.1, 25.0, 26.1, 30.9, 33.1,"
				+ " 33.1, 34.0, 36.0, 43.0, 48.0, 55.0, 61.0, 62.1, 64.0,"
				+ " 64.0, 69.1, 77.0";
		String[] testA = test.split(",");
		for(String a : testA){
			serverDataRecordPivotValuesList.add(Double.valueOf(a.trim()));
		}
		int numberOfProcessors = 2;
	
		List<Double> pivArray = new ArrayList<Double>();
		int interval = serverDataRecordPivotValuesList.size() / numberOfProcessors;
		int count = 1;
		for (int i = interval; i < serverDataRecordPivotValuesList.size() && 
				count < numberOfProcessors; i += interval) {
			pivArray.add(serverDataRecordPivotValuesList.get(i));
			count ++;
		}
		
		System.out.println("pivArray "+ pivArray);
		
	}
}
