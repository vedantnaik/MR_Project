package coolmapreduce;

import java.io.File;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.List;

import word.count.WordCount.TokenizerMapper;

public class MapperHandler implements Runnable {

	/**
	 * TODO: MapperHandler Requirement
	 * 
	 * 1. We need Job's information 2. For that Job, get the Mapper InputPath
	 * Context
	 * 
	 * 3. For each file - Read [see format, based on that reader to get each
	 * line] - For each line, call map on Mapper implementation Send context
	 * along
	 * 
	 * 4. Notify main class after all lines from all files are read.
	 * 
	 * */

	List<File> listOfMapperFiles;
	Job currentJob;

	// Assumes each MapperHanlder has a list of files to work
	// on
	public MapperHandler(List<File> _files, Job _job) {
		listOfMapperFiles = _files;
		currentJob = _job;
	}

	@Override
	public void run() {
		// starts running the mapper phase
		Context con = new Context();

		Class cls = null;
		Object obj = null;
		String classname = "";
		try {
			classname = currentJob.getMapperClass().getName();
			cls = Class.forName(currentJob.getMapperClass().getName());
			obj = cls.newInstance();
		} catch (ClassNotFoundException e1) {
			System.out.println("Error finding class in JVM " + classname);
			e1.printStackTrace();
		} catch (InstantiationException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IllegalAccessException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		System.out.println("Calling functions for "
				+ currentJob.getMapperClass().toString());
		// setup phase
		try {

			System.out.println("calling setup for class " + cls);

			Method setup = cls.getMethod("setup", Context.class);

			System.out.println("calling setup " + setup);

			// give context variable
			setup.invoke(obj, con);

		} catch (NoSuchMethodException | SecurityException e) {

			System.out.println("exception in init of setup");
			e.printStackTrace();
		} catch (IllegalAccessException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IllegalArgumentException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (InvocationTargetException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		try {

			System.out.println("calling cleanup for class "
					+ currentJob.getMapperClass().toString());

			Method cleanup = cls.getMethod("cleanup", Context.class);

			System.out.println("methods " + cleanup);

			// give context variable
			cleanup.invoke(obj, con);

		} catch (NoSuchMethodException | SecurityException e) {

			System.out.println("exception in init of cleaup");
			e.printStackTrace();
		} catch (IllegalAccessException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IllegalArgumentException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (InvocationTargetException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	public static void main(String[] args) {

		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf);
		job.setMapperClass(TokenizerMapper.class);
		MapperHandler mh = new MapperHandler(null, job);
		mh.run();
	}
}
