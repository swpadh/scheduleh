import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;


public class FlightJoinKeyPartitioner extends Partitioner<FlightJoinKey, Text>{

	@Override
	public int getPartition(FlightJoinKey key, Text value, int numPartition) {
		return Math.abs(key.hashCode() * 127) % numPartition;
	}
}
