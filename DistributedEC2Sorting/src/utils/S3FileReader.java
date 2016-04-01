package utils;

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.util.zip.GZIPInputStream;

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.S3Object;

/**
 * File reader for a file on a S3 bucket.
 * */
public class S3FileReader {

	String bucketName;
	String fileObjectKey;
	Reader s3FileReader;
	
	public S3FileReader(String bucketName, String fileObjectKey) throws IOException {
		this.bucketName = bucketName;
		this.fileObjectKey = fileObjectKey;
		
		AWSCredentials credentials = new ProfileCredentialsProvider().getCredentials();
		AmazonS3 s3client = new AmazonS3Client(credentials);
		S3Object s3object = s3client.getObject(new GetObjectRequest(bucketName, fileObjectKey));
		InputStream gzipStream = new GZIPInputStream(s3object.getObjectContent());
		
		this.s3FileReader = new InputStreamReader(gzipStream, "ASCII");
		
		s3object = null;
		s3client = null;
		credentials = null;
	}
	
	public String readFromOffsetToLen(long fromChar, int recordLength) throws IOException {
		char[] cbuf = new char[1000];
		
		this.s3FileReader.skip(fromChar);
		this.s3FileReader.read(cbuf, 0, recordLength);
		
		this.s3FileReader = new S3FileReader(this.bucketName, this.fileObjectKey).getS3FileReader();
		
		return new String(cbuf);
	}	
		
	// Getters and Setters
	
	public String getBucketName() {
		return bucketName;
	}

	public void setBucketName(String bucketName) {
		this.bucketName = bucketName;
	}

	public String getFileObjectKey() {
		return fileObjectKey;
	}

	public void setFileObjectKey(String fileObjectKey) {
		this.fileObjectKey = fileObjectKey;
	}

	public Reader getS3FileReader() {
		return s3FileReader;
	}

	public void setS3FileReader(Reader s3FileReader) {
		this.s3FileReader = s3FileReader;
	}
}
