import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class FlightScheduleReducer extends
		Reducer<FlightJoinKey, Text, Text, NullWritable> {

	@Override
	protected void reduce(FlightJoinKey flightKey, Iterable<Text> flightInfo,
			Reducer<FlightJoinKey, Text, Text, NullWritable>.Context context)
			throws IOException, InterruptedException {
		Iterator<Text> iter = flightInfo.iterator();
		String strDelay = "";

		int arrivalDelay = Integer.parseInt(flightKey.getArrivalDelay().toString());
		if (arrivalDelay == Integer.MAX_VALUE) {
			strDelay = iter.next().toString();		
		}
		while (iter.hasNext()) {
				StringBuilder strBuilder = new StringBuilder();
				Text record = iter.next();
				strBuilder.append(record.toString());
				strBuilder.append(",");
				strBuilder.append(strDelay);
				context.write(new Text(strBuilder.toString()),
						NullWritable.get());
		}

	}

}
