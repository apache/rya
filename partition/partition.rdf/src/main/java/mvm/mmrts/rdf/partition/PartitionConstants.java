package mvm.mmrts.rdf.partition;

import cloudbase.core.CBConstants;
import cloudbase.core.data.Value;
import cloudbase.core.security.Authorizations;
import mvm.mmrts.rdf.partition.query.operators.ShardSubjectLookup;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.openrdf.model.Literal;
import org.openrdf.model.URI;
import org.openrdf.model.ValueFactory;
import org.openrdf.model.impl.ValueFactoryImpl;

import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * Class PartitionConstants
 * Date: Jul 6, 2011
 * Time: 12:22:55 PM
 */
public class PartitionConstants {

    public static final String PARTITION_NS = "urn:mvm.mmrts.partition.rdf/08/2011#";
    public static ValueFactory VALUE_FACTORY = ValueFactoryImpl.getInstance();
    public static URI TIMERANGE = VALUE_FACTORY.createURI(PARTITION_NS, "timeRange");
    public static URI SHARDRANGE = VALUE_FACTORY.createURI(PARTITION_NS, "shardRange"); //shardRange(subject, start, stop) in ms
    public static Literal EMPTY_LITERAL = VALUE_FACTORY.createLiteral(0);

    public static final byte FAMILY_DELIM = 0;
    public static final String FAMILY_DELIM_STR = "\0";
    public static final byte INDEX_DELIM = 1;
    public static final String INDEX_DELIM_STR = "\1";

    /* RECORD TYPES */
//    public static final int NAMESPACE_MARKER = 2;
//
//    public static final int EXPL_TRIPLE_MARKER = 3;
//
//    public static final int EXPL_QUAD_MARKER = 4;
//
//    public static final int INF_TRIPLE_MARKER = 5;
//
//    public static final int INF_QUAD_MARKER = 6;

    public static final int URI_MARKER = 7;

    public static final String URI_MARKER_STR = "\07";

    public static final int BNODE_MARKER = 8;

    public static final int PLAIN_LITERAL_MARKER = 9;

    public static final String PLAIN_LITERAL_MARKER_STR = "\u0009";

    public static final int LANG_LITERAL_MARKER = 10;

    public static final int DATATYPE_LITERAL_MARKER = 11;

    public static final String DATATYPE_LITERAL_MARKER_STR = "\u000B";

    public static final int EOF_MARKER = 127;

    //	public static final Authorizations ALL_AUTHORIZATIONS = new Authorizations(
    //	"_");
    public static final Authorizations ALL_AUTHORIZATIONS = CBConstants.NO_AUTHS;

    public static final Value EMPTY_VALUE = new Value(new byte[0]);
    public static final Text EMPTY_TXT = new Text("");

    /* Column Families and Qualifiers */
    public static final Text INDEX = new Text("index");
    public static final Text DOC = new Text("event");
    public static final Text NAMESPACE = new Text("ns");

    /* Time constants */
    public static final String START_BINDING = "binding.start";
    public static final String END_BINDING = "binding.end";
    public static final String TIME_PREDICATE = "binding.timePredicate";
    public static final String SHARDRANGE_BINDING = "binding.shardRange";
    public static final String SHARDRANGE_START = "binding.shardRange.start";
    public static final String SHARDRANGE_END = "binding.shardRange.end";
    public static final String TIME_TYPE_PROP = "binding.timeProp";
    public static final String AUTHORIZATION_PROP = "binding.authorization";
    public static final String NUMTHREADS_PROP = "binding.numthreads";
    public static final String ALLSHARDS_PROP = "binding.allshards";

    public static final String VALUE_DELIMITER = "\03";

    public static final SimpleDateFormat XMLDATE = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'");

    public enum TimeType {
        TIMESTAMP, XMLDATETIME
    }

    public static boolean isTimeRange(ShardSubjectLookup lookup, Configuration configuration) {
        return (configuration.get(TIME_PREDICATE) != null) || (lookup.getTimePredicate() != null);
    }

    public static Long validateFillStartTime(Long start, ShardSubjectLookup lookup) {
        if (lookup.getShardStartTimeRange() != null)
            return Long.parseLong(lookup.getShardEndTimeRange());
        return (start == null) ? 0l : start;
    }

    public static Long validateFillEndTime(Long end, ShardSubjectLookup lookup) {
        if (lookup.getShardEndTimeRange() != null)
            return Long.parseLong(lookup.getShardEndTimeRange());
        return (end == null) ? System.currentTimeMillis() : end;
    }

    public static String getStartTimeRange(ShardSubjectLookup lookup, Configuration configuration) {
        String tp = configProperty(configuration, TIME_PREDICATE, lookup.getTimePredicate());
        String st = configProperty(configuration, START_BINDING, lookup.getStartTimeRange());
        TimeType tt = lookup.getTimeType();
        if (tt == null)
            tt = TimeType.valueOf(configuration.get(TIME_TYPE_PROP));
        return URI_MARKER_STR + tp + INDEX_DELIM_STR + convertTime(Long.parseLong(st), tt);
    }

    public static String getEndTimeRange(ShardSubjectLookup lookup, Configuration configuration) {
        String tp = configProperty(configuration, TIME_PREDICATE, lookup.getTimePredicate());
        String et = configProperty(configuration, END_BINDING, lookup.getEndTimeRange());
        TimeType tt = lookup.getTimeType();
        if (tt == null)
            tt = TimeType.valueOf(configuration.get(TIME_TYPE_PROP));
        return URI_MARKER_STR + tp + INDEX_DELIM_STR + convertTime(Long.parseLong(et), tt);
    }

    public static String convertTime(Long timestamp, TimeType timeType) {
        return (TimeType.XMLDATETIME.equals(timeType))
                ? (DATATYPE_LITERAL_MARKER_STR + XMLDATE.format(new Date(timestamp)))
                : PLAIN_LITERAL_MARKER_STR + timestamp;
    }

    public static String configProperty(Configuration configuration, String property, String checkValue) {
        if (checkValue == null)
            return configuration.get(property);
        return checkValue;
    }
}
