import java.net.URI;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class MapSideJoinJobDC extends Configured implements Tool {

	public static void main(String[] args) throws Exception {
		int status = ToolRunner.run(new MapSideJoinJobDC(), args);
		System.exit(status);
	}

	@Override
	public int run(String[] arg0) throws Exception {
		Job job = new Job(getConf(), " Mapside join with Distributed Cache");
		Path inputPath = new Path("input");
		Path outputPath = new Path("output");

		FileSystem fs = FileSystem.get(getConf());
		if (fs.exists(outputPath)) {
			fs.delete(outputPath);
		}

		job.setJarByClass(getClass());
		FileInputFormat.addInputPath(job, inputPath);
		job.setInputFormatClass(TextInputFormat.class);
		job.setMapperClass(MapSideJoinDCMapper.class);
		job.setMapOutputKeyClass(NullWritable.class);
		job.setMapOutputValueClass(Text.class);
		
		job.addCacheFile(new URI("/tmp/carriers.txt"));
		
		job.setNumReduceTasks(0);
		TextOutputFormat.setOutputPath(job, outputPath);
		
		return job.waitForCompletion(true) ? 0 : 1;
	}

}
