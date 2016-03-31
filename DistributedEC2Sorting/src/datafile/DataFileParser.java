package datafile;

import java.util.ArrayList;
import java.util.Arrays;

public class DataFileParser {

	public class Field{
		public static final String DRY_BULB_TEMP = "DryBulbTemp";
		public static final String WBAN_NUMBER = "WbanNumber";
		public static final String YEARMONTHDAY = "YearMonthDay";
		public static final String TIME = "Time";
	}
	
	public static final String[] csvh = {"WbanNumber","YearMonthDay","Time","StationType",
			"MaintenanceIndicator","SkyConditions","Visibility","WeatherType","DryBulbTemp",
			"DewPointTemp","WetBulbTemp","percentRelativeHumidity","WindSpeedKT","WindDirection",
			"WindCharGustsKT","ValforWindChar","StationPressure","PressureTendency",
			"SeaLevelPressure","RecordType","PrecipTotal" };
	public static final ArrayList<String> csvHeaders = new ArrayList<String>(Arrays.asList(csvh));

	
	// TODO: add sanity checks
	public static boolean isRecordValid(String[] fields){
		if(fields.length != 21){
			return false;
		}
		
		if(null == DataFileParser.getValueOf(fields, Field.DRY_BULB_TEMP)){
			return false;
		}
		if(DataFileParser.getValueOf(fields, Field.DRY_BULB_TEMP).equalsIgnoreCase("")){
			return false;
		}
		
		try {
			Double.parseDouble(DataFileParser.getValueOf(fields, Field.DRY_BULB_TEMP));
		} catch(NumberFormatException e) {
			return false;
		}
		
		return true;
	}
	
	public static String getValueOf(String[] fields, String headerName){
		return fields[csvHeaders.indexOf(headerName)];
	}
	
}
