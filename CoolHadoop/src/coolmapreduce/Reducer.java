package coolmapreduce;

import java.io.IOException;

public abstract class Reducer<KI, VI, KO, VO> {
	
	protected abstract void setup(Context context);
	
	// TODO: set generics using input/output classes, and context
	public abstract void reduce(KI key, Iterable<VI> value, Context context) 
			throws IOException, InterruptedException;
	
	protected abstract void cleanup(Context context);
	
}