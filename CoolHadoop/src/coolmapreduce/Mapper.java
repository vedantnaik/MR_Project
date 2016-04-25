package coolmapreduce;

import java.io.IOException;

/**
 * 
 * @author Dixit_Patel
 *
 * @param <KI> Key In
 * @param <VI> Value In
 * @param <KO> Key Out 
 * @param <VO> Value Out
 */
public abstract class Mapper<KI, VI, KO, VO>{
	
	protected void setup(Context context) throws Exception{
		// noop
	}
	
	public abstract void map(KI key, VI value, Context context) throws IOException, InterruptedException;
	
	protected void cleanup(Context context) throws Exception{
		//noop
	}
	
}
