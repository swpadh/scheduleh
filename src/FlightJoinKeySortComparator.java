import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class FlightJoinKeySortComparator extends WritableComparator {

	protected FlightJoinKeySortComparator() {
		super(FlightJoinKey.class, true);
	}

	@Override
	public int compare(WritableComparable a, WritableComparable b) {
		FlightJoinKey key1 = (FlightJoinKey) a;
		FlightJoinKey key2 = (FlightJoinKey) b;

		return FlightJoinKey.compare(key1, key2);
	}

}
