import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;


public class FlightJoinKeyGroupComparator extends WritableComparator{

	protected FlightJoinKeyGroupComparator()
	{
		super(FlightJoinKey.class, true);
	}

	@Override
	public int compare(WritableComparable a, WritableComparable b) {
		FlightJoinKey key1 = (FlightJoinKey) a;
		FlightJoinKey key2 = (FlightJoinKey) b;
		int cmp = key1.getYear().compareTo(key2.getYear());
		if (cmp != 0) {
			return cmp;
		}
		cmp = key1.getMonth().compareTo(key2.getMonth());
		if (cmp != 0) {
			return cmp;
		}
		cmp = key1.getDayOfMonth().compareTo(key2.getDayOfMonth());
			return cmp;
	}

}
