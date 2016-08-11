import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class WeatherMapper extends
		Mapper<LongWritable, Text, FlightJoinKey, Text> {

	@Override
	protected void map(LongWritable key, Text value,
			Mapper<LongWritable, Text, FlightJoinKey, Text>.Context context)
			throws IOException, InterruptedException {
		try {
			String[] weatherInfo = value.toString().split(",");
			if (weatherInfo.length < 23)
				return;
			String date = weatherInfo[0];
			if (date.indexOf("/") == -1)
				return;
			String[] strDate = date.split("/");
			String year = strDate[2];
			String month = strDate[0];
			String dayOfMonth = strDate[1];

			String maxTemp = weatherInfo[1];
			String minTemp = weatherInfo[2];
			String precipitation = weatherInfo[19];

			StringBuilder strWeather = new StringBuilder();
			strWeather.append(maxTemp);
			strWeather.append(",");
			strWeather.append(minTemp);
			strWeather.append(",");
			strWeather.append(precipitation);
			strWeather.append(",");

			context.write(new FlightJoinKey(month, dayOfMonth, year, String.valueOf(Integer.MAX_VALUE)),
					new Text(strWeather.toString()));
		} catch (Exception ex) {
			ex.printStackTrace();
			throw ex;
		}
	}

}
