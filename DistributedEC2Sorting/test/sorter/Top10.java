package sorter;

import java.util.ArrayList;
import java.util.Collections;

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.AccessControlList;
import com.amazonaws.services.s3.model.GroupGrantee;
import com.amazonaws.services.s3.model.ObjectListing;
import com.amazonaws.services.s3.model.Permission;
import com.amazonaws.services.s3.model.S3ObjectSummary;

import datafile.DataRecord;
import utils.FileSystem;

public class Top10 {

	
	public static void main(String[] args) {
		
		
		
		AWSCredentials credentials = new ProfileCredentialsProvider().getCredentials();
		AmazonS3 s3client = new AmazonS3Client(credentials);
		
		ArrayList<DataRecord> top10fromalllist = new ArrayList<DataRecord>(); 
		
		ObjectListing objList = s3client.listObjects("cs6240sp16");
	
		int count = 1;
		for(S3ObjectSummary objSum : objList.getObjectSummaries() ){
			if(objSum.getKey().contains("climate") && objSum.getKey().contains("txt.gz")){
				ArrayList<DataRecord> thisFileDRList = FileSystem.readInputDataRecordsFromInputBucket("cs6240sp16", objSum.getKey());
				System.out.println(objSum.getKey() + " : " + thisFileDRList.get(0) + " to " + thisFileDRList.get(thisFileDRList.size()-2) + ", " + thisFileDRList.get(thisFileDRList.size()-1));
				Collections.sort(thisFileDRList);
				System.out.println(objSum.getKey() + " : " + thisFileDRList.get(0) + " to " + thisFileDRList.get(thisFileDRList.size()-2) + ", " + thisFileDRList.get(thisFileDRList.size()-1));
				
				top10fromalllist.addAll(thisFileDRList.subList(thisFileDRList.size() - 11, thisFileDRList.size() ));
				System.out.println(count + " files done");
				count++;
			}
		}

		
		Collections.sort(top10fromalllist);
		
		for(DataRecord dr : top10fromalllist){
			System.out.println(dr.getWban() + "," + dr.getDate() + "," + dr.getTime() + "," + dr.getSortValue());
		}
	}
	
	
}
