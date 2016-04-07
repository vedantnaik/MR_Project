package sorter;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.Map;
import java.util.zip.GZIPInputStream;

public class Reader {
	
	public static void main(String[] args) throws IOException {

		File folder = new File("X://climate//");
		File[] listOfFiles = folder.listFiles();
		Map<Double, Integer> temp_count = new HashMap<>(50000000);
		BufferedWriter bufWriter = new BufferedWriter(new FileWriter(
				"counter.csv"));
		int co = 0;
		for (int i = 0; i < listOfFiles.length; i++) {
			co ++;
			System.out.println("counter " + co + "size " + temp_count.size());
			InputStream fStream = new FileInputStream(listOfFiles[i]);
			fStream = new GZIPInputStream(fStream);
			InputStreamReader gzreader = new InputStreamReader(fStream, "ASCII");

			
			BufferedReader br = new BufferedReader(gzreader);
			String line = "";
			String split[];
			while (null != (line = br.readLine())) {
				try {
					split = line.split(",");
//					if (split.length == 21) {
						double t = Double.parseDouble(split[10]);
						if (temp_count.containsKey(t)) {
							temp_count.put(t, temp_count.get(t) + 1);
						} else {
							temp_count.put(t, 1);
						}
//					}
				} catch (Exception e) {

				}
			}
			br.close();
			
			// 3. close streams and delete temp file
		}
		
		System.out.println("size " + temp_count.size());
		for (Double d : temp_count.keySet()) {
			bufWriter.write(d + ", " + temp_count.get(d) + "\n");
		}
		bufWriter.close();
		
	}
}
