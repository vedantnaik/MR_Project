package fs;

import utils.Constants;

public class Path {

	String curDir;
	String separator = Constants.UNIX_FILE_SEPARATOR;
	
	public Path(String _curDir) {
		this.curDir = _curDir;
	}
	
	@Override
	public String toString() {
		return this.curDir;
	}
}
