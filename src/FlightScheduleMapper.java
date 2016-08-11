import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.log4j.Logger;

public class FlightScheduleMapper extends
		Mapper<LongWritable, Text, FlightJoinKey, Text> {
	@Override
	protected void map(LongWritable key, Text value,
			Mapper<LongWritable, Text, FlightJoinKey, Text>.Context context)
			throws IOException, InterruptedException {

			String[] strValue = value.toString().split(",");
			if(strValue.length < 28)
			{
				return;
			}
			String year = strValue[0];
			String month = strValue[1];
			String dayOfMonth = strValue[2];

			String depTime = strValue[4];
			String arrTime = strValue[6];
			String uniqueCarrier = strValue[8];
			String flightNum = strValue[9];
			String actualElapsedTime = strValue[11];
			String arrivalDelay = strValue[14];
			String depDelay = strValue[15];
			String origin = strValue[16];
			String destn = strValue[17];

			if (destn.equals("SFO")) {

				StringBuilder flightInfoBuilder = new StringBuilder(year);
				flightInfoBuilder.append(",");
				flightInfoBuilder.append(month);
				flightInfoBuilder.append(",");
				flightInfoBuilder.append(dayOfMonth);
				flightInfoBuilder.append(",");
				flightInfoBuilder.append(depTime);
				flightInfoBuilder.append(",");
				flightInfoBuilder.append(arrTime);
				flightInfoBuilder.append(",");
				flightInfoBuilder.append(uniqueCarrier);
				flightInfoBuilder.append(",");
				flightInfoBuilder.append(flightNum);
				flightInfoBuilder.append(",");
				flightInfoBuilder.append(actualElapsedTime);
				flightInfoBuilder.append(",");
				flightInfoBuilder.append(arrivalDelay);
				flightInfoBuilder.append(",");
				flightInfoBuilder.append(depDelay);
				flightInfoBuilder.append(",");
				flightInfoBuilder.append(origin);
				flightInfoBuilder.append(",");
				flightInfoBuilder.append(destn);

				context.write(new FlightJoinKey(month, dayOfMonth, year,
						arrivalDelay), new Text(flightInfoBuilder.toString()));
			}
	}
}
