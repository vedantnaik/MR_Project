package coolmapreduce;


import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;

import utils.Constants;
import master.Master;

public class Job implements Serializable{

	
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	String jobName;
	
	public static Configuration conf;
	Class jarByClass;
	Class mapperClass;
	Class reducerClass;
	Class mapOutputKeyClass;
	Class mapOutputValueClass;
	
	
	Class<?> outputKeyClass;
	Class<?> outputValueClass;
	
	//singleton class which returns job object itself 
	//This will have getInstance which takes config object
	private Job(){
	}
	
	public static Job getInstance(Configuration _conf){
		if(ref == null) {
			ref = new Job();
		}		
		// Instantiate Job class	
		conf = _conf;
		return ref;
	}
	
	private static Job ref = new Job();
	
	public String getJobName() {
		return jobName;
	}

	public void setJobName(String jobName) {
		this.jobName = jobName;
	}

	public static Configuration getConf() {
		return conf;
	}

	public void setConf(Configuration _conf) {
		conf = _conf;
	}

	public Class getJarByClass() {
		return jarByClass;
	}

	public void setJarByClass(Class jarByClass) {
		this.jarByClass = jarByClass;
	}

	public Class getMapperClass() {
		return mapperClass;
	}

	public void setMapperClass(Class mapperClass) {
		this.mapperClass = mapperClass;
	}

	public Class getReducerClass() {
		return reducerClass;
	}

	public void setReducerClass(Class reducerClass) {
		this.reducerClass = reducerClass;
	}

	public Class getMapOutputKeyClass() {
		return mapOutputKeyClass;
	}

	public void setMapOutputKeyClass(Class mapOutputKeyClass) {
		this.mapOutputKeyClass = mapOutputKeyClass;
	}

	public Class getMapOutputValueClass() {
		return mapOutputValueClass;
	}

	public void setMapOutputValueClass(Class mapOutputValueClass) {
		this.mapOutputValueClass = mapOutputValueClass;
	}

	public void setOutputKeyClass(Class<?> _outputKeyClass){
		outputKeyClass = _outputKeyClass;
	}
	
	public void setOutputValueClass(Class<?> _outputValueClass){
		outputValueClass = _outputValueClass;
	}
	
	public Class<?> getOutputKeyClass(){
		return outputKeyClass;
	}
	
	public Class<?> getOutputValueClass(){
		return outputValueClass;
	}
	
	@Override
	public String toString() {
		return "Configuration: " + getConf() + "\n" + " JobName " + getJobName();
	}
	
	/**
	 * Submit the job to the slave servers and wait for it to finish.
	 * @param verbose print the progress to the user
	 * @return true if the job succeeded
	 * @throws IOException 
	 */

	public boolean waitForCompletion(boolean verbose) throws IOException {
		// TODO:
		// write "this" object to file (jobfile_jobname)
		// Tried in test program, works
		
		// Call the Master(Client Program here)
		// the client connects to all servers, ships
		// the jobfile_jobname
		// starts Map and co-ordinates till end of job 
		// to return true
		
		System.out.println("serializing this " + this);
		// serialize job
		ObjectOutputStream oos = new ObjectOutputStream(
				new FileOutputStream(this.getJobName()));
        oos.writeObject(this);
        oos.flush();
        oos.close();
		
        // TODO: Master by default is local, specify false if on Server
		Master master = new Master();
		
		// TODO: Split the path's and startJob should return bool
		master.startJob(this, getConf().get(Constants.CTX_INPUT_PATH_KEY), 
				"", 
				getConf().get(Constants.CTX_OUTPUT_PATH_KEY), 
				"");
	
		return true;
	}
	
//	public boolean waitForCompletion2() throws FileNotFoundException, IOException, ClassNotFoundException{
//		
//
//		String str = "testtext";
//		
//		ObjectOutputStream oos = new ObjectOutputStream(new FileOutputStream(this.getJobName()));
//        oos.writeObject(this);
//        oos.flush();
//        oos.close();
//
//		System.out.println("End Receiver");
//		System.out.println("reading it back");
//		
//		ObjectInputStream iis = new ObjectInputStream(new FileInputStream(new File(this.getJobName())));
//		Job test = (Job) iis.readObject();
//		System.out.println("test Job " + test);
//		iis.close();
//		
//		return true;
//	}
//	
//	public static void main(String[] args) throws FileNotFoundException, ClassNotFoundException, IOException {
//		Configuration _conf = new Configuration();
//		_conf.set("WHO", "SERVER");
//		Job obj = Job.getInstance(_conf);
//		obj.setJobName("mytest");	
//		if(obj.waitForCompletion2()){
//			System.out.println("true");
//		}
//	}
}
