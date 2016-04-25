package coolmapreduce.lib.output;


import coolmapreduce.Job;
import fs.Path;
import utils.Constants;
/**
 * 
 * @author Vedant_Naik
 *
 */
public class FileOutputFormat {
	public static void setOutputPath(Job myJob, Path outputPath){
		myJob.getConf().set(Constants.CTX_OUTPUT_PATH_KEY, outputPath.toString());
	}
}
