package ss.cloudbase.core.iterators;

import org.apache.hadoop.io.Text;

import cloudbase.core.data.Value;

public class IteratorConstants {
	public static final byte[] emptyByteArray = new byte[0];
	public static final Value emptyValue = new Value(emptyByteArray);
	public static final Text emptyText = new Text(emptyByteArray);
}
