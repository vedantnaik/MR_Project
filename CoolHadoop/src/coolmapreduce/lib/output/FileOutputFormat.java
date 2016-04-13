package coolmapreduce.lib.output;


import coolmapreduce.Job;
import fs.Path;
import utils.Constants;

public class FileOutputFormat {
	
	
	
	//FileOutputFormat.setOutputPath(job, new Path(args[1]));
	public static void setOutputPath(Job myJob, Path outputPath){
		myJob.getConf().set(Constants.CTX_OUTPUT_PATH_KEY, outputPath.toString());
	}
	
	
	
	
}
