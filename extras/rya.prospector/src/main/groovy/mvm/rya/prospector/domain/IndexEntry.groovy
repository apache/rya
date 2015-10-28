package mvm.rya.prospector.domain

/**
 * Date: 12/5/12
 * Time: 11:33 AM
 */
class IndexEntry {
    def String index
    def String data
    def String dataType
    def String tripleValueType
    def String visibility
    def Long count
    def Long timestamp

    @Override
    public String toString() {
        return "IndexEntry{" +
                "index='" + index + '\'' +
                ", data='" + data + '\'' +
                ", dataType='" + dataType + '\'' +
                ", tripleValueType=" + tripleValueType +
                ", visibility='" + visibility + '\'' +
                ", timestamp='" + timestamp + '\'' +
                ", count=" + count +
                '}';
    }

    boolean equals(o) {
        if (this.is(o)) return true
        if (getClass() != o.class) return false

        IndexEntry that = (IndexEntry) o

        if (count != that.count) return false
        if (timestamp != that.timestamp) return false
        if (data != that.data) return false
        if (dataType != that.dataType) return false
        if (index != that.index) return false
        if (tripleValueType != that.tripleValueType) return false
        if (visibility != that.visibility) return false

        return true
    }

    int hashCode() {
        int result
        result = (index != null ? index.hashCode() : 0)
        result = 31 * result + (data != null ? data.hashCode() : 0)
        result = 31 * result + (dataType != null ? dataType.hashCode() : 0)
        result = 31 * result + (tripleValueType != null ? tripleValueType.hashCode() : 0)
        result = 31 * result + (visibility != null ? visibility.hashCode() : 0)
        result = 31 * result + (count != null ? count.hashCode() : 0)
        result = 31 * result + (timestamp != null ? timestamp.hashCode() : 0)
        return result
    }
}
