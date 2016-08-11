import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

public class FlightJoinKey implements WritableComparable<FlightJoinKey> {

	private Text year;
	private Text month;
	private Text dayOfMonth;
	private Text arrivalDelay;


	public FlightJoinKey() {
		this.year = new Text();
		this.month = new Text();
		this.dayOfMonth = new Text();
		this.arrivalDelay = new Text();
	}

	public FlightJoinKey(String month, String dayOfMonth, String year,
			String arrivalDelay) {

		this.year = new Text(year);
		this.month = new Text(month);
		this.dayOfMonth = new Text(dayOfMonth);
		this.arrivalDelay = new Text(arrivalDelay);
	}

	@Override
	public void readFields(DataInput dataIn) throws IOException {
		year.readFields(dataIn);
		month.readFields(dataIn);
		dayOfMonth.readFields(dataIn);
		arrivalDelay.readFields(dataIn);
	}

	@Override
	public void write(DataOutput dataOut) throws IOException {
		year.write(dataOut);
		month.write(dataOut);
		dayOfMonth.write(dataOut);
		arrivalDelay.write(dataOut);
	}

	@Override
	public int compareTo(FlightJoinKey key) {
		int cmp = compare(year.toString(), key.year.toString());
		if (cmp != 0) {
			return cmp;
		}
		cmp = compare(month.toString(), key.month.toString());
		if (cmp != 0) {
			return cmp;
		}
		cmp = compare(dayOfMonth.toString(), key.dayOfMonth.toString());
		if (cmp != 0) {
			return cmp;
		}
		cmp = compare(arrivalDelay.toString(), key.arrivalDelay.toString());
		return cmp;
	}

	public static int compare(String stra, String strb) {
		int a = 0;
		int b = 0;
		try {
			a = Integer.parseInt(stra);
		} catch (Exception ex) {
		}
		try {
			b = Integer.parseInt(strb);
		} catch (Exception ex) {
		}

		return (a < b ? -1 : (a == b ? 0 : 1));
	}

	public static int compare(FlightJoinKey key1, FlightJoinKey key2) {
		int cmp = compare(key1.year.toString(), key2.year.toString());
		if (cmp != 0) {
			return cmp;
		}
		cmp = compare(key1.month.toString(), key2.month.toString());
		if (cmp != 0) {
			return cmp;
		}
		cmp = compare(key1.dayOfMonth.toString(), key2.dayOfMonth.toString());
		if (cmp != 0) {
			return cmp;
		}
		cmp = compare(key1.arrivalDelay.toString(),
				key2.arrivalDelay.toString());
		return (-1) * cmp;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		FlightJoinKey other = (FlightJoinKey) obj;
		if (arrivalDelay == null) {
			if (other.arrivalDelay != null)
				return false;
		} else if (!arrivalDelay.equals(other.arrivalDelay))
			return false;
		if (dayOfMonth == null) {
			if (other.dayOfMonth != null)
				return false;
		} else if (!dayOfMonth.equals(other.dayOfMonth))
			return false;
		if (month == null) {
			if (other.month != null)
				return false;
		} else if (!month.equals(other.month))
			return false;
		if (year == null) {
			if (other.year != null)
				return false;
		} else if (!year.equals(other.year))
			return false;
		return true;
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result
				+ ((dayOfMonth == null) ? 0 : dayOfMonth.hashCode());
		result = prime * result + ((month == null) ? 0 : month.hashCode());
		result = prime * result + ((year == null) ? 0 : year.hashCode());
		return result;
	}

	@Override
	public String toString() {
		return "FlightKey [year=" + year + ", month=" + month + ", dayOfMonth="
				+ dayOfMonth + ", arrivalDelay=" + arrivalDelay + "]";
	}

	public Text getYear() {
		return year;
	}

	public Text getMonth() {
		return month;
	}

	public Text getDayOfMonth() {
		return dayOfMonth;
	}

	public Text getArrivalDelay() {
		return arrivalDelay;
	}
	
	public String getDate()
	{
		StringBuilder strDate = new StringBuilder();
		strDate.append(year);
		strDate.append(",");
		strDate.append(month);
		strDate.append(",");
		strDate.append(dayOfMonth);
		return strDate.toString();
	}
}
