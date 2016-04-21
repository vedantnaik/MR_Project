package coolmapreduce;

import java.io.IOException;

public abstract class Reducer<KI, VI, KO, VO> {
	
	protected void setup(Context context) throws Exception{
		//noop?
	}
	
	// TODO: set generics using input/output classes, and context
	protected abstract void reduce(KI key, Iterable<VI> value, Context context) 
			throws IOException, InterruptedException;
	
	protected void cleanup(Context context) throws Exception{
		//noop?
	}
	
}