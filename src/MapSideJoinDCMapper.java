import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.net.URI;
import java.util.HashMap;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class MapSideJoinDCMapper extends
		Mapper<LongWritable, Text, NullWritable, Text> {

	private HashMap<String, String> careerInfoMap = new HashMap<String, String>();

	@Override
	protected void cleanup(
			Mapper<LongWritable, Text, NullWritable, Text>.Context context)
			throws IOException, InterruptedException {
		careerInfoMap.clear();
		careerInfoMap = null;
		super.cleanup(context);
	}

	@Override
	protected void map(LongWritable key, Text value,
			Mapper<LongWritable, Text, NullWritable, Text>.Context context)
			throws IOException, InterruptedException {
		String strValue = value.toString();
		int airlineIndex = strValue.indexOf(",");
		String airLinesInfo = strValue.substring(airlineIndex + 1);
		String airLineCode = strValue.substring(0, airlineIndex);
		if (careerInfoMap.get(airLineCode) != null) {
			StringBuilder strBuilder = new StringBuilder(
					careerInfoMap.get(airLineCode));
			strBuilder.append(",");
			strBuilder.append(airLinesInfo);
			context.write(NullWritable.get(), new Text(strBuilder.toString()));
		}
	}

	@Override
	protected void setup(
			Mapper<LongWritable, Text, NullWritable, Text>.Context context)
			throws IOException, InterruptedException {
		URI[] cacheFiles = context.getCacheFiles();
		for (URI afile : cacheFiles) {
			String path = afile.getPath();
			if (path.contains("carriers.txt")) {
				loadCacheFile(path, context);
			}
		}
		super.setup(context);
	}

	private void loadCacheFile(String path, Context context) throws IOException {
		BufferedReader bufReader = null;
		try {
			//FileSystem fs = FileSystem.get(context.getConfiguration());
			//fs.open(f)
			bufReader = new BufferedReader(new FileReader(path));
			String line = "";
			while ((line = bufReader.readLine()) != null) {
				String[] careerInfo = line.split(",");
				if (careerInfo.length > 1) {
					careerInfoMap.put(careerInfo[0], careerInfo[1]);
				}
			}
		} finally {
			if (bufReader != null) {
				bufReader.close();
			}
		}
	}
}