package mvm.mmrts.rdf.partition;

/**
 * Class InvalidValueTypeMarkerRuntimeException
 * Date: Jan 7, 2011
 * Time: 12:58:27 PM
 */
public class InvalidValueTypeMarkerRuntimeException extends RuntimeException {
    private int valueTypeMarker = -1;

    public InvalidValueTypeMarkerRuntimeException(int valueTypeMarker) {
        super();
        this.valueTypeMarker = valueTypeMarker;
    }

    public InvalidValueTypeMarkerRuntimeException(int valueTypeMarker, String s) {
        super(s);
        this.valueTypeMarker = valueTypeMarker;
    }

    public InvalidValueTypeMarkerRuntimeException(int valueTypeMarker, String s, Throwable throwable) {
        super(s, throwable);
        this.valueTypeMarker = valueTypeMarker;
    }

    public InvalidValueTypeMarkerRuntimeException(int valueTypeMarker, Throwable throwable) {
        super(throwable);
        this.valueTypeMarker = valueTypeMarker;
    }

    public int getValueTypeMarker() {
        return valueTypeMarker;
    }
}
