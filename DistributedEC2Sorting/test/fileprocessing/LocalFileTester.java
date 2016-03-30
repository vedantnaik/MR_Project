package fileprocessing;

import java.io.File;
import java.io.FileInputStream;
import java.io.ObjectInputStream;
import java.util.ArrayList;

import datafile.DataRecord;

public class LocalFileTester  {

	public static void main(String[] args) throws Exception{
		File folderIn = new File(0+"/");
		
		
		for(File partFile : folderIn.listFiles()){
			
			FileInputStream fileStream = new FileInputStream(folderIn+"/"+partFile.getName());
			ObjectInputStream ois = new ObjectInputStream(fileStream);
			ArrayList<DataRecord> drList = (ArrayList<DataRecord>) ois.readObject();
			
			for(DataRecord dr : drList){
				System.out.println(dr.getSortValue());
			}
			System.out.println("====================");
		}
		
	}
	
}
