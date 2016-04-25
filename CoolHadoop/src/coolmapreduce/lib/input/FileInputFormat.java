package coolmapreduce.lib.input;



import coolmapreduce.Job;
import fs.Path;
import utils.Constants;
/**
 * 
 * @author Vedant_Naik
 *
 */
public class FileInputFormat {
	public static void addInputPath(Job myJob, Path inputPath){
		myJob.getConf().set(Constants.CTX_INPUT_PATH_KEY, inputPath.toString());
	}
}
