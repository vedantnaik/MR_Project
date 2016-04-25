package coolmapreduce;

import java.io.IOException;

public abstract class Mapper<KI, VI, KO, VO>{
	
	protected void setup(Context context) throws Exception{
		// noop
	}
	
	public abstract void map(KI key, VI value, Context context) throws IOException, InterruptedException;
	
	protected void cleanup(Context context) throws Exception{
		//noop
	}
	
}
