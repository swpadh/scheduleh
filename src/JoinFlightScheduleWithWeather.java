import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;


public class JoinFlightScheduleWithWeather extends Configured implements Tool{

	@Override
	public int run(String[] args) throws Exception {
		args = new String[3];
		
	   Job job = new Job(getConf(),"Join Flight Record delays with weather");
	  //args[0] = "hdfs://sandbox.hortonworks.com:8020/tmp/admin/data/2008.csv";
	 // args[1] = "hdfs://sandbox.hortonworks.com:8020/tmp/admin/data/Weather 2008.csv";
	  //args[2] = "hdfs://sandbox.hortonworks.com:8020/tmp/admin/data/output";
	   args[0] = "input/2008.csv";
	   args[1] = "input/Weather 2008.csv";
	   args[2] = "output";
	   
	   Path flightInputPath = new Path(args[0]);
	   Path weatherInputPath = new Path(args[1]);
	   Path outputPath = new Path(args[2]);
	   
	   FileSystem fs = FileSystem.get(getConf());
	   if(fs.exists(outputPath))
	   {
		   fs.delete(outputPath);
	   }
	   job.setJarByClass(getClass());
	   //Input and Output Path
	   MultipleInputs.addInputPath(job,flightInputPath ,TextInputFormat.class, FlightScheduleMapper.class);
	   MultipleInputs.addInputPath(job, weatherInputPath, TextInputFormat.class, WeatherMapper.class);
	   //TextInputFormat.setInputPaths(job, flightInputPath);
	   //TextInputFormat.setInputPaths(job, weatherInputPath);
	   //job.setInputFormatClass(TextInputFormat.class);
	   job.setMapOutputKeyClass(FlightJoinKey.class);
	   job.setMapOutputValueClass(Text.class);
	   FileOutputFormat.setOutputPath(job, outputPath);
	   
	   //Key Connection
	   //job.setMapperClass(FlightScheduleMapper.class);
	   //job.setMapperClass(WeatherMapper.class);
	   //job.setMapOutputKeyClass(FlightJoinKey.class);
	   //job.setMapOutputValueClass(Text.class);
	   
	   job.setReducerClass(FlightScheduleReducer.class);	   
	   job.setOutputKeyClass(Text.class);
	   job.setOutputValueClass(NullWritable.class);
	   
	   //Partitioner and Grouping and Sort Comparator
	   
	   job.setPartitionerClass(FlightJoinKeyPartitioner.class);
	   job.setSortComparatorClass(FlightJoinKeySortComparator.class);
	   job.setGroupingComparatorClass(FlightJoinKeyGroupComparator.class);
	
	   return job.waitForCompletion(true)? 0:1;
	}
   public static void main(String[] args) throws Exception
   {
	   int exitCode = ToolRunner.run(new JoinFlightScheduleWithWeather(), args);
	   System.exit(exitCode);
   }
}
