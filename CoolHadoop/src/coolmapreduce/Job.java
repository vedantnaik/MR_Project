package coolmapreduce;

import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.net.SocketException;
import java.util.Map;

import com.jcraft.jsch.JSchException;
import com.jcraft.jsch.SftpException;

import fs.FileSys;
import master.Master;
import utils.Constants;

/**
 * Job class which is Serializable Contains the configuration object
 */
public class Job implements Serializable {

	/**
	 * Serial version UID
	 */
	private static final long serialVersionUID = 1L;

	String jobName;

	public Configuration conf;
	Class<?> jarByClass;

	// Mapper/Reducer classes
	Class<?> mapperClass;
	Class<?> reducerClass;

	// Mapper Output key,value
	Class<?> mapOutputKeyClass;
	Class<?> mapOutputValueClass;

	// Reducer Output key,value
	Class<?> outputKeyClass;
	Class<?> outputValueClass;

	/**
	 * Private Job <init> so no one else can init these except from getInstance
	 * functions.
	 */
	private Job() {
	}

	/**
	 * Private Job <init> which initializes with a conf object as specified.
	 * 
	 * @param _conf
	 *            the job object with which Job is needed
	 */
	private Job(Configuration _conf) {
		this.conf = _conf;
	}

	/**
	 * Gets the Job instance with the configuration as specified
	 * 
	 * @param _conf
	 *            the configuration Object
	 * @return Job instance
	 */
	public static Job getInstance(Configuration _conf) {
		return new Job(_conf);
	}

	// private Job ref = new Job();

	public String getJobName() {
		return jobName;
	}

	public void setJobName(String jobName) {
		this.jobName = jobName;
	}

	public Configuration getConf() {
		return conf;
	}

	public void setConf(Configuration _conf) {
		conf = _conf;
	}

	public Class<?> getJarByClass() {
		return jarByClass;
	}

	public void setJarByClass(Class<?> jarByClass) {
		this.jarByClass = jarByClass;
	}

	public Class<?> getMapperClass() {
		return mapperClass;
	}

	public void setMapperClass(Class<?> mapperClass) {
		this.mapperClass = mapperClass;
	}

	public Class<?> getReducerClass() {
		return reducerClass;
	}

	public void setReducerClass(Class<?> reducerClass) {
		this.reducerClass = reducerClass;
	}

	public Class<?> getMapOutputKeyClass() {
		return mapOutputKeyClass;
	}

	public void setMapOutputKeyClass(Class<?> mapOutputKeyClass) {
		this.mapOutputKeyClass = mapOutputKeyClass;
	}

	public Class<?> getMapOutputValueClass() {
		return mapOutputValueClass;
	}

	public void setMapOutputValueClass(Class<?> mapOutputValueClass) {
		this.mapOutputValueClass = mapOutputValueClass;
	}

	public void setOutputKeyClass(Class<?> _outputKeyClass) {
		outputKeyClass = _outputKeyClass;
	}

	public void setOutputValueClass(Class<?> _outputValueClass) {
		outputValueClass = _outputValueClass;
	}

	public Class<?> getOutputKeyClass() {
		return outputKeyClass;
	}

	public Class<?> getOutputValueClass() {
		return outputValueClass;
	}

	@Override
	public String toString() {
		return "Configuration: " + getConf() + "\n" + " JobName "
				+ getJobName();
	}

	/**
	 * Submit the job to the slave servers and wait for it to finish.
	 * 
	 * @param verbose
	 *            print the progress to the user
	 * @return true if the job succeeded
	 * @throws IOException
	 * @throws SftpException 
	 * @throws JSchException 
	 */

	public boolean waitForCompletion(boolean verbose) throws IOException, JSchException, SftpException  {
		// TODO:
		// write "this" object to file (jobfile_jobname)
		// Tried in test program, works

		System.out.println("serializing this " + this);
		
		String jobFilename = this.getJobName() + Constants.JOBEXTN;
		serializeThisAsFilename(this, jobFilename);
		moveToSlaves(jobFilename);

		// TODO: Master by default is local, specify false if on Server
		Master master = new Master(getConf(), false);

		// TODO: Split the path's and startJob should return boolean
		master.startJob(this, getConf().get(Constants.CTX_INPUT_PATH_KEY), "",
				getConf().get(Constants.CTX_OUTPUT_PATH_KEY), "");

		return true;
	}
	
	public void serializeThisAsFilename(Object thisVar, String asFileName) throws IOException{
		// serialize job
		ObjectOutputStream oos = new ObjectOutputStream(new FileOutputStream(
				asFileName));
		oos.writeObject(this);
		oos.flush();
		oos.close();
	}
	
	public void moveToSlaves(String filename) throws SocketException, IOException, JSchException, SftpException{
		
		for(Map.Entry<Integer, String> ip : getConf().getServerIPaddrMap().entrySet()){
			
			String fullPath = Constants.PROJECT_HOME + filename;
			System.out.println("moving " + filename + " to " + 
					fullPath + " @ " + ip.getValue());
			FileSys.scpCopy(filename, fullPath, ip.getValue());
		}
		
	}

}
