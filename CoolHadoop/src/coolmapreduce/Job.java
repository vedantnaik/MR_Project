package coolmapreduce;
public class Job {
	
	
	String jobName;
	
	static Configuration conf;
	Class jarByClass;
	Class mapperClass;
	Class reducerClass;
	Class mapOutputKeyClass;
	Class mapOutputValueClass;
	
	
	//singleton class which returns job object itself 
	//This will have getInstance which takes config object
	private Job(){}
	
	public static Job getInstance(Configuration _conf){
		if(ref == null) {
			ref = new Job();
		}
		
		// Instantiate Job class
		
		conf = _conf;
		
		
		return ref;
	}
	
	private static Job ref;

	// TODO: wait for completion <<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<
	// waitForCompletion(true)
	
	
	
	public String getJobName() {
		return jobName;
	}

	public void setJobName(String jobName) {
		this.jobName = jobName;
	}

	public static Configuration getConf() {
		return conf;
	}

	public static void setConf(Configuration conf) {
		Job.conf = conf;
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

	
}
