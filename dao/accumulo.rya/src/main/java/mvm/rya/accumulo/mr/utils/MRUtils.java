package mvm.rya.accumulo.mr.utils;

/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * 
 *   http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */



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

    public static final String AC_USERNAME_PROP = "ac.username";
    public static final String AC_PWD_PROP = "ac.pwd";
    public static final String AC_ZK_PROP = "ac.zk";
    public static final String AC_INSTANCE_PROP = "ac.instance";
    public static final String AC_TTL_PROP = "ac.ttl";
    public static final String AC_TABLE_PROP = "ac.table";
    public static final String AC_AUTH_PROP = "ac.auth";
    public static final String AC_CV_PROP = "ac.cv";
    public static final String AC_MOCK_PROP = "ac.mock";
    public static final String AC_HDFS_INPUT_PROP = "ac.hdfsinput";
    public static final String HADOOP_IO_SORT_MB = "ac.hdfsinput";
    public static final String TABLE_LAYOUT_PROP = "rdf.tablelayout";
    public static final String FORMAT_PROP = "rdf.format";
    public static final String INPUT_PATH = "input";

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


    public static String getACTtl(Configuration conf) {
        return conf.get(AC_TTL_PROP);
    }

    public static String getACUserName(Configuration conf) {
        return conf.get(AC_USERNAME_PROP);
    }

    public static String getACPwd(Configuration conf) {
        return conf.get(AC_PWD_PROP);
    }

    public static String getACZK(Configuration conf) {
        return conf.get(AC_ZK_PROP);
    }

    public static String getACInstance(Configuration conf) {
        return conf.get(AC_INSTANCE_PROP);
    }

    public static void setACUserName(Configuration conf, String str) {
        conf.set(AC_USERNAME_PROP, str);
    }

    public static void setACPwd(Configuration conf, String str) {
        conf.set(AC_PWD_PROP, str);
    }

    public static void setACZK(Configuration conf, String str) {
        conf.set(AC_ZK_PROP, str);
    }

    public static void setACInstance(Configuration conf, String str) {
        conf.set(AC_INSTANCE_PROP, str);
    }

    public static void setACTtl(Configuration conf, String str) {
        conf.set(AC_TTL_PROP, str);
    }
}
