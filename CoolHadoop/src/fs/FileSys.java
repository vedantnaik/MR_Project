package fs;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.OutputStream;
import java.io.Reader;
import java.util.ArrayList;
import java.util.zip.GZIPInputStream;

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.S3Object;

import io.Text;
import utils.Constants;

public class FileSys {
	
	/**
	 * Read from S3 bucket
	 * 
	 * File type is .gz
	 * 
	 * @param 
	 * inputBucketName : S3 bucket where input files are stored
	 * fileObjectkey : An input file object key
	 * 
	 * @return
	 * An ArrayList of Strings where each line from input file is stored in the list		  	
	 * */
	public static ArrayList<String> readGZippedInputStringsFromInputS3Bucket(String inputBucketName, String fileObjectKey){
		ArrayList<String> dataRecordList = new ArrayList<String>();
		try {
			AWSCredentials credentials = new ProfileCredentialsProvider().getCredentials();
			AmazonS3 s3client = new AmazonS3Client(credentials);
			S3Object s3object = s3client.getObject(new GetObjectRequest(inputBucketName, fileObjectKey));
			
			GZIPInputStream gzipStream = new GZIPInputStream(s3object.getObjectContent());
			BufferedReader buffered = new BufferedReader(new InputStreamReader(gzipStream, "ASCII"));
			
			String fileLine;
			while((fileLine = buffered.readLine())!=null){
				dataRecordList.add(fileLine);
			}
			
			buffered.close();
			gzipStream.close();
		} catch (IOException e) {
			System.out.println("Unable to read file from Input S3 Bucket");
			e.printStackTrace();
		}
		
		return dataRecordList;
	}
	
	/**
	 * 
	 * Read from local file systems
	 * 
	 * File type is .gz
	 * 
	 * @param 
	 * inputBucketName : S3 bucket where input files are stored
	 * fileObjectkey : An input file object key
	 * 
	 * @return
	 * An ArrayList of Strings where each line from input file is stored in the list
	 * */
	public static ArrayList<String> readInputStringsFromLocalInputBucket(String folderPath, String fileName){
		ArrayList<String> dataRecordList = new ArrayList<String>();
		String fileToReadName = folderPath + "/" + fileName;
		try {
			GZIPInputStream gzipStream = new GZIPInputStream(new FileInputStream(new File(fileToReadName)));
			BufferedReader reader = new BufferedReader(new InputStreamReader(gzipStream, "ASCII"));
			String fileLine;
			
			while(null != (fileLine = reader.readLine())){
				dataRecordList.add(fileLine);
			}
			
			reader.close();
			gzipStream.close();
		} catch (FileNotFoundException e) {
			System.err.println("Unable to read local file " + fileToReadName);
			e.printStackTrace();
		} catch (IOException e) {
			System.err.println("Unable to read line from local copy of file " + fileToReadName);
			e.printStackTrace();
		}
		return dataRecordList;
	}
	
	
	/**
	 * Write local file
	 * USAGE: recording output form Mapper : context.write 
	 *  
	 * ./output/<JOBNAME>/mapper/<KEY>/values.txt
	 * */
	public static void writeMapperValueToKeyFolder(Text key, Text value, String jobName){
		
		String fileNameToWriteIn = Constants.MAPPER_CONTEXT_OUTPUT_FILE
									.replace("<JOBNAME>", jobName)
									.replace("<KEY>", key.toString());
		
		File folderToWriteIn = new File(fileNameToWriteIn.replace("/values.txt", ""));
		if(!folderToWriteIn.isDirectory()){
			folderToWriteIn.mkdir();
			try {
				ObjectOutputStream oos = getOOS(new File(fileNameToWriteIn));
		        oos.writeObject(value);
		        oos.flush();
		        oos.close();
			} catch (IOException e) {
				System.out.println("Could not do a context write from mapper");
				e.printStackTrace();
			}
		} else {
			// Folder exists. Key previously seen on this EC2
			try {
				ObjectOutputStream oos = getOOS(new File(fileNameToWriteIn));
		        oos.writeObject(value);
		        oos.flush();
		        oos.close();
		    } catch (IOException e) {
				System.out.println("Could not do a context write from mapper");
				e.printStackTrace();
			}
		}
		
	}

	/**
	 * Read from local file
	 * Assuming moved all temp files to this EC2 instance's folder structure
	 * ./input/<JOBNAME>/reducer/<KEY>/values-<serverNumber>.txt
	 * 
	 * USAGE: Feeding input to reduce  
	 *  
	 * */
	public static void readMapperOutputForKey(Text key, String jobName){
		// TODO: 1. Add filepath to constants
		// TODO: 2. Cater to multiple input files
		
		String fileToRead = "./input/"+jobName+"/reducer/"+key.toString()+"/values.txt";
		
		try {
			FileInputStream fileStream = new FileInputStream(fileToRead);
			ObjectInputStream ois = getOIS(fileStream);

			System.out.println(ois.readObject().toString());
			System.out.println(ois.readObject().toString());
		
			ois.close();
			fileStream.close();
		} catch (IOException e) {
			System.err.println("Unable to read temp output from Mapper. File: " + fileToRead);
			e.printStackTrace();
		} catch (ClassNotFoundException e) {
			System.err.println("Unable to deserialize the object from file : " + fileToRead);
			e.printStackTrace();
		}
		
		
	}	
	
	
	
	
	/**
	 * Class used to append a serializable object to file
	 * 
	 * http://stackoverflow.com/questions/2094637/how-can-i-append-to-an-existing-java-io-objectstream
	 * */
	
	private static ObjectOutputStream getOOS(File storageFile) throws IOException {
		if (storageFile.exists()) {
		    // this is a workaround so that we can append objects to an existing file
		    return new AppendableObjectOutputStream(new FileOutputStream(storageFile, true));
		} else {
		    return new ObjectOutputStream(new FileOutputStream(storageFile));
		}
	}
	
	private static class AppendableObjectOutputStream extends ObjectOutputStream {

	    public AppendableObjectOutputStream(OutputStream out) throws IOException {
	        super(out);
	    }

	    @Override
	    protected void writeStreamHeader() throws IOException {
	        // do not write a header
	    }
	}

	/**
	 * Used to read appended serializable object from file
	 * 
	 * http://stackoverflow.com/questions/2094637/how-can-i-append-to-an-existing-java-io-objectstream
	 * */
	
	private static ObjectInputStream getOIS(FileInputStream fis)
            throws IOException {
		long pos = fis.getChannel().position();
		return pos == 0 ? new ObjectInputStream(fis) : 
		    new AppendableObjectInputStream(fis);
	}
	
	private static class AppendableObjectInputStream extends ObjectInputStream {

        public AppendableObjectInputStream(InputStream in) throws IOException {
            super(in);
        }

        @Override
        protected void readStreamHeader() throws IOException {
            // do not read a header
        }
    }
}

