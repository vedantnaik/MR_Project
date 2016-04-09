package coolmapreduce;

import java.io.IOException;

public abstract class Mapper<KI, VI, KO, VO> {
	
	protected abstract void setup(Context context);
	
	// TODO: set generics using input/output classes, and context
	public abstract void map(KI key, VI value, Context context) throws IOException, InterruptedException;
	
	protected abstract void cleanup(Context context);
	
}
