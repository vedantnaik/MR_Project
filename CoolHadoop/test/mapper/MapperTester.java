package mapper;

import java.io.File;
import java.util.ArrayList;
import java.util.List;

import utils.Constants;
import word.count.WordCount.TokenizerMapper;
import coolmapreduce.Configuration;
import coolmapreduce.Job;
import coolmapreduce.MapperHandler;

public class MapperTester {

	public static void main(String[] args) {
		
		List<File> files = new ArrayList<>();
		files.add(new File("alice.txt.gz"));
		
		for(int i = 0 ; i< 100; i ++)
			files.add(new File("alice.txt.gz"));

		Configuration conf = new Configuration();
		conf.set(Constants.INPUT_BUCKET_NAME, "resources");
		Job job = Job.getInstance(conf);
		job.setMapperClass(TokenizerMapper.class);
		MapperHandler mh = new MapperHandler(files, job);
		mh.run();
		
		
	}
}
