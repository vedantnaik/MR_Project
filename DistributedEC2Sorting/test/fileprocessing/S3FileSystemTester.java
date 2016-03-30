package fileprocessing;

import java.util.ArrayList;
import java.util.HashMap;

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.ObjectListing;
import com.amazonaws.services.s3.model.S3ObjectSummary;

import utils.FileSystem;

public class S3FileSystemTester {

	
	public static void main(String[] args) {
		
		FileSystem myfs = new FileSystem("cs6240sp16");
		
		HashMap<Integer, ArrayList<String>> partMap = myfs.getS3Parts(2);
		
		for(Integer key : partMap.keySet()){
			System.out.println(partMap.get(key).size() + " " + partMap.get(key));
		}
		
	}
}
