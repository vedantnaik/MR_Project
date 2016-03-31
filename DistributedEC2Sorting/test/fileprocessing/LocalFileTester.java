package fileprocessing;

import java.io.File;
import java.io.FileInputStream;
import java.io.ObjectInputStream;
import java.util.ArrayList;

import datafile.DataRecord;

public class LocalFileTester  {

	public static void main(String[] args) throws Exception{
		String fromServer_s = "2";
		String serverNumber_p = "1";
		String fileName = fromServer_s+"/"+"s"+fromServer_s+"p"+serverNumber_p+".txt";
		
		

//		S3FileReader s3fr = new S3FileReader("cs6240sp16", "climate/199703hourly.txt.gz");
//		ArrayList<DataRecord> drListFor199703 = FileSystem.readRecordsFrom("cs6240sp16", "climate/199703hourly.txt.gz");
//	
//		
//		File f = null;
//		
//		f = new File(fromServer_s + "");
//		f.mkdir();
//		
//		FileOutputStream fout = new FileOutputStream(fileName);
//		ObjectOutputStream oos = new ObjectOutputStream(fout);
//		oos.writeObject(drListFor199703);
		
		int count = 0;
		File folderIn = new File("sampleSortMyParts/");
		
		for(File partFile : folderIn.listFiles()){
			
			System.out.println("file : " + partFile.getName());
			FileInputStream fileStream = new FileInputStream(folderIn+"/"+partFile.getName());
			ObjectInputStream ois = new ObjectInputStream(fileStream);
			ArrayList<DataRecord> drList = (ArrayList<DataRecord>) ois.readObject();
			
			for(DataRecord dr : drList){
				System.out.println("====================");
				System.out.println(dr.getSortValue());
				count++;
			}
		}
		
		
		
		System.out.println("All count : " + count);
	}
	
}
