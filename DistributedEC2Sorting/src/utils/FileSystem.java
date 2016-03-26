package utils;

import java.io.File;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class FileSystem {
	double summation = 0.0;
	int PARTS;
	public FileSystem() {
		// noop
	}
	
	/**
	 * returns the List<File> given a directory location
	 * @param dir a string directory location 
	 * @return returns List<File> underneath that location/dir
	 * @throws Exception
	 */
	public List<FileType> getFiles(String dir) throws Exception {
		summation = 0.0;
		List<FileType> list = new ArrayList<>();
		addFile(new FileType(new File(dir)), list);
		return list;
	}
	/**
	 * recursive helper function for getFiles function
	 * to read the files beneath folder of folder.
	 * @param fileVar is the file a directory? if yes read more 
	 * and add to the list arg as argument
	 * @param list the list arg as argument
	 */
	private void addFile(FileType fileVar, List<FileType> list) {
		if (fileVar.file.isDirectory()) {
			String[] subNodes = fileVar.file.list();
			for (String subNode : subNodes) {
				addFile(new FileType(fileVar.file, subNode), list);
			}
		} else {
			list.add(fileVar);
			summation += fileVar.size;
		}
	}
	
	/**
	 * mega function to read files and call makePartition to make partitions
	 * of the data 
	 * @param the location of files to read from
	 * @param number the of partitions to part files into  
	 * @return List of List of partitions of equal size
	 * @throws Exception
	 */
	private List<List<FileType>> readFilesGetPartition(String location, int number) throws Exception {
		PARTS = number + 1;
		List<FileType> sortedFiles = getFiles(location);
		Collections.sort(sortedFiles);
		List<List<FileType>> partitions = makePartition(sortedFiles, PARTS);
		return partitions;
	}

	/**
	 * makes partition in number of numberOfParts based on size
	 * so each partition has almost equal size 
	 * @param sortedFiles files in sorted order
	 * @param numberOfParts number of parts in which all files are needed
	 * @return List of List of partitions of equal size
	 */
	private List<List<FileType>> makePartition(List<FileType> sortedFiles,
			int numberOfParts) {
		double eachPartSize = summation/numberOfParts;
		double eachPartSummation = 0.0;
		List<List<FileType>> partitions = new ArrayList<>();
		List<FileType> current = new ArrayList<>();
		for(FileType f : sortedFiles){			
			if(eachPartSummation >= eachPartSize){
				System.out.println("size " + eachPartSummation);
				eachPartSummation =  0.0;
				partitions.add(current);
				current = new ArrayList<>();
			}
			eachPartSummation += f.size;
			current.add(f);
		}
		return partitions;
	}

	public static void main(String[] args) throws Exception {
		FileSystem fs = new FileSystem();
		System.out.println("files: " + fs.getFiles("X://climate//"));
		System.out.println("summation: " + fs.summation);
		System.out.println("sets: " + fs.readFilesGetPartition("X://climate//", 8));
	}
}
