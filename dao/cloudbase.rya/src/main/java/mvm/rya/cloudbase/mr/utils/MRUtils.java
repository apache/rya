package mvm.rya.cloudbase.mr.utils;

import org.apache.hadoop.conf.Configuration;
import org.openrdf.model.URI;
import org.openrdf.model.ValueFactory;
import org.openrdf.model.impl.ValueFactoryImpl;

/**
 * Class MRSailUtils
 * Date: May 19, 2011
 * Time: 10:34:06 AM
 */
public class MRUtils {

    public static final String JOB_NAME_PROP = "mapred.job.name";

    public static final String CB_USERNAME_PROP = "cb.username";
    public static final String CB_PWD_PROP = "cb.pwd";
    public static final String CB_ZK_PROP = "cb.zk";
    public static final String CB_INSTANCE_PROP = "cb.instance";
    public static final String CB_TTL_PROP = "cb.ttl";
    public static final String CB_CV_PROP = "cb.cv";
    public static final String CB_AUTH_PROP = "cb.auth";
    public static final String CB_MOCK_PROP = "cb.mock";
    public static final String TABLE_LAYOUT_PROP = "rdf.tablelayout";
    public static final String FORMAT_PROP = "rdf.format";

    public static final String NAMED_GRAPH_PROP = "rdf.graph";

    public static final String TABLE_PREFIX_PROPERTY = "rdf.tablePrefix";

    // rdf constants
    public static final ValueFactory vf = new ValueFactoryImpl();
    public static final URI RDF_TYPE = vf.createURI("http://www.w3.org/1999/02/22-rdf-syntax-ns#", "type");


    // cloudbase map reduce utils

//    public static Range retrieveRange(URI entry_key, URI entry_val) throws IOException {
//        ByteArrayDataOutput startRowOut = ByteStreams.newDataOutput();
//        startRowOut.write(RdfCloudTripleStoreUtils.writeValue(entry_key));
//        if (entry_val != null) {
//            startRowOut.write(RdfCloudTripleStoreConstants.DELIM_BYTES);
//            startRowOut.write(RdfCloudTripleStoreUtils.writeValue(entry_val));
//        }
//        byte[] startrow = startRowOut.toByteArray();
//        startRowOut.write(RdfCloudTripleStoreConstants.DELIM_STOP_BYTES);
//        byte[] stoprow = startRowOut.toByteArray();
//
//        Range range = new Range(new Text(startrow), new Text(stoprow));
//        return range;
//    }


    public static String getCBTtl(Configuration conf) {
        return conf.get(CB_TTL_PROP);
    }

    public static String getCBUserName(Configuration conf) {
        return conf.get(CB_USERNAME_PROP);
    }

    public static String getCBPwd(Configuration conf) {
        return conf.get(CB_PWD_PROP);
    }

    public static String getCBZK(Configuration conf) {
        return conf.get(CB_ZK_PROP);
    }

    public static String getCBInstance(Configuration conf) {
        return conf.get(CB_INSTANCE_PROP);
    }

    public static void setCBUserName(Configuration conf, String str) {
        conf.set(CB_USERNAME_PROP, str);
    }

    public static void setCBPwd(Configuration conf, String str) {
        conf.set(CB_PWD_PROP, str);
    }

    public static void setCBZK(Configuration conf, String str) {
        conf.set(CB_ZK_PROP, str);
    }

    public static void setCBInstance(Configuration conf, String str) {
        conf.set(CB_INSTANCE_PROP, str);
    }

    public static void setCBTtl(Configuration conf, String str) {
        conf.set(CB_TTL_PROP, str);
    }
}
