package coolmapreduce;

import java.io.IOException;

public abstract class Mapper<KI, VI, KO, VO> {
	
	public void setup(Context context){
		//noop??
	}
	
	// TODO: set generics using input/output classes, and context
	public abstract void map(KI key, VI value, Context context) throws IOException, InterruptedException;
	
	public void cleanup(Context context){
		//noop??
	}
	
}
