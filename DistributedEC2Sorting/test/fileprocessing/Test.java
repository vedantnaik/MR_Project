package fileprocessing;

import java.io.File;
import java.io.IOException;

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.AccessControlList;
import com.amazonaws.services.s3.model.GroupGrantee;
import com.amazonaws.services.s3.model.Permission;
import com.amazonaws.services.s3.model.PutObjectRequest;
import com.amazonaws.services.s3.model.PutObjectResult;

public class Test {

	public static void main(String[] args) throws IOException {
		AWSCredentials credentials = new ProfileCredentialsProvider().getCredentials();
		AmazonS3 s3client = new AmazonS3Client(credentials);
		AccessControlList acl;
		acl = new AccessControlList();
		acl.grantPermission(GroupGrantee.AllUsers, Permission.Read);
		
		int serverNumber = 9;
		String outputBucketName = "distributedsort";
		String serverNum = serverNumber+"";
		String fileNameOnS3Bucket = "DistributedEC2Sort"
				+"/part-"+("00000" + serverNum).substring(serverNum.length());
		System.out.println("writePartsToOutputBucket moving to s3 " + outputBucketName);
		PutObjectResult result = s3client.putObject(new PutObjectRequest(outputBucketName, 
				fileNameOnS3Bucket, new File("MYPARTSSORTEDCOMPLETE")).withAccessControlList(acl));
		System.out.println("result1 " + result);			
		
		System.out.println("writePartsToOutputBucket moved to s3");

	}
}
