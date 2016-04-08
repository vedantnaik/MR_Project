package coolmapreduce.lib.input;

import java.nio.file.Path;

import coolmapreduce.Configuration;
import coolmapreduce.Job;
import utils.Constants;

public class FileInputFormat {
	
	
	
	//FileInputFormat.addInputPath(job, new Path(args[0]));
	public static void addInputPath(Job myJob, Path inputPath){
		myJob.getConf().set(Constants.CTX_INPUT_PATH_KEY, inputPath.toString());
	}
	
	
	
	
}
