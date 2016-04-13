package coolmapreduce.lib.input;



import coolmapreduce.Job;
import fs.Path;
import utils.Constants;

public class FileInputFormat {
	
	
	
	//FileInputFormat.addInputPath(job, new Path(args[0]));
	public static void addInputPath(Job myJob, Path inputPath){
		Job.getConf().set(Constants.CTX_INPUT_PATH_KEY, inputPath.toString());
		
		// setting for input bucket as well
		Job.getConf().set(Constants.INPUT_BUCKET_NAME, inputPath.toString());

				
	}
	
	
}
