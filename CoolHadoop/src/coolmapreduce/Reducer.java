package coolmapreduce;

import java.io.IOException;
/**
 * 
 * @author Dixit_Patel
 *
 * @param <KI> key In
 * @param <VI> Value In
 * @param <KO> Key Out
 * @param <VO> Value Out
 */
public abstract class Reducer<KI, VI, KO, VO> {
	
	protected void setup(Context context) throws Exception{
		//noop
	}
	
	public abstract void reduce(KI key, Iterable<VI> value, Context context) 
			throws IOException, InterruptedException;
	
	protected void cleanup(Context context) throws Exception{
		//noop
	}
	
}