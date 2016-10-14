package mvm.rya.accumulo.mr;

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

import org.apache.accumulo.core.client.Instance;
import org.apache.accumulo.core.client.mapreduce.InputFormatBase;
import org.apache.accumulo.core.client.security.tokens.AuthenticationToken;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.openrdf.model.ValueFactory;
import org.openrdf.model.impl.ValueFactoryImpl;
import org.openrdf.rio.RDFFormat;

import mvm.rya.api.RdfCloudTripleStoreConstants.TABLE_LAYOUT;

/**
 * Contains constants and static methods for interacting with a
 * {@link Configuration} and handling options likely to be relevant to Rya
 * MapReduce jobs. Defines constant property names associated with Accumulo and
 * Rya options, and some convenience methods to get and set these properties
 * with respect to a given Configuration.
 */
public class MRUtils {
    /**
     * Property name for the name of a MapReduce job.
     */
    public static final String JOB_NAME_PROP = "mapred.job.name";

    /**
     * Property name for the Accumulo username.
     */
    public static final String AC_USERNAME_PROP = "ac.username";
    /**
     * Property name for the Accumulo password.
     */
    public static final String AC_PWD_PROP = "ac.pwd";

    /**
     * Property name for the list of zookeepers.
     */
    public static final String AC_ZK_PROP = "ac.zk";
    /**
     * Property name for the Accumulo instance name.
     */
    public static final String AC_INSTANCE_PROP = "ac.instance";
    /**
     * Property name for whether to run against a mock Accumulo instance.
     */
    public static final String AC_MOCK_PROP = "ac.mock";

    /**
     * Property name for TTL; allows using an age-off filter on Accumulo input.
     */
    public static final String AC_TTL_PROP = "ac.ttl";

    /**
     * Property name for scan authorizations when reading data from Accumulo.
     */
    public static final String AC_AUTH_PROP = "ac.auth";
    /**
     * Property name for default visibility when writing data to Accumulo.
     */
    public static final String AC_CV_PROP = "ac.cv";

    /**
     * Property name for whether to read Accumulo data directly from HDFS
     * as opposed to through Accumulo itself.
     */
    public static final String AC_HDFS_INPUT_PROP = "ac.hdfsinput";
    /**
     * Property name for the table layout to use when reading data from Rya.
     */
    public static final String TABLE_LAYOUT_PROP = "rdf.tablelayout";

    /**
     * Property name for the Rya table prefix, identifying the Rya
     * instance to work with.
     */
    public static final String TABLE_PREFIX_PROPERTY = "rdf.tablePrefix";
    /**
     * Property name for the RDF serialization format to use, when using RDF
     * files.
     */
    public static final String FORMAT_PROP = "rdf.format";
    /**
     * Property name for a file input path, if using file input.
     */
    public static final String INPUT_PATH = "input";
    /**
     * Property name for specifying a default named graph to use when writing
     * new statements.
     */
    public static final String NAMED_GRAPH_PROP = "rdf.graph";

    public static final String AC_TABLE_PROP = "ac.table";
    public static final String HADOOP_IO_SORT_MB = "io.sort.mb";
    public static final ValueFactory vf = new ValueFactoryImpl();

    /**
     * Gets the TTL from a given Configuration.
     * @param conf  Configuration containing MapReduce tool options.
     * @return  The TTL that will be applied as an age-off filter for Accumulo
     *          input data, or null if not set.
     */
    public static String getACTtl(Configuration conf) {
        return conf.get(AC_TTL_PROP);
    }

    /**
     * Gets the username from a given Configuration.
     * @param conf  Configuration containing MapReduce tool options.
     * @return  The configured Accumulo username, or null if not set.
     */
    public static String getACUserName(Configuration conf) {
        return conf.get(AC_USERNAME_PROP);
    }

    /**
     * Gets the password from a given Configuration.
     * @param conf  Configuration containing MapReduce tool options.
     * @return  The configured Accumulo password, or null if not set.
     */
    public static String getACPwd(Configuration conf) {
        return conf.get(AC_PWD_PROP);
    }

    /**
     * Gets the zookeepers from a given Configuration.
     * @param conf  Configuration containing MapReduce tool options.
     * @return  The configured zookeeper list, or null if not set.
     */
    public static String getACZK(Configuration conf) {
        return conf.get(AC_ZK_PROP);
    }

    /**
     * Gets the instance name from a given Configuration.
     * @param conf  Configuration containing MapReduce tool options.
     * @return  The configured Accumulo instance name, or null if not set.
     */
    public static String getACInstance(Configuration conf) {
        return conf.get(AC_INSTANCE_PROP);
    }

    /**
     * Gets whether to use a mock instance from a given Configuration.
     * @param conf          Configuration containing MapReduce tool options.
     * @param defaultValue  Default choice if the mock property hasn't been
     *                      explicitly set in the Configuration.
     * @return  True if a mock instance should be used, false to connect to
     *          a running Accumulo.
     */
    public static boolean getACMock(Configuration conf, boolean defaultValue) {
        return conf.getBoolean(AC_MOCK_PROP, defaultValue);
    }

    /**
     * Gets the table prefix from a given Configuration.
     * @param conf  Configuration containing MapReduce tool options.
     * @return  The configured Rya table prefix, or null if not set.
     */
    public static String getTablePrefix(Configuration conf) {
        return conf.get(TABLE_PREFIX_PROPERTY);
    }

    /**
     * Gets the table layout that determines which Rya table to scan for input.
     * @param   conf            Configuration containing MapReduce tool options.
     * @param   defaultLayout   The layout to use if the Configuration doesn't
     *                          specify any layout.
     * @return  The configured layout to use for reading statements from Rya.
     */
    public static TABLE_LAYOUT getTableLayout(Configuration conf, TABLE_LAYOUT defaultLayout) {
        return TABLE_LAYOUT.valueOf(conf.get(TABLE_LAYOUT_PROP, defaultLayout.toString()));
    }

    /**
     * Gets the RDF serialization format to use for parsing RDF files.
     * @param   conf    Configuration containing MapReduce tool options.
     * @return  The configured RDFFormat, or null if not set.
     */
    public static RDFFormat getRDFFormat(Configuration conf) {
        return RDFFormat.valueOf(conf.get(FORMAT_PROP));
    }

    /**
     * Sets the username in the given Configuration.
     * @param conf  Configuration containing MapReduce tool options.
     * @param str   Accumulo username, used for input and/or output.
     */
    public static void setACUserName(Configuration conf, String str) {
        conf.set(AC_USERNAME_PROP, str);
    }

    /**
     * Sets the password in the given Configuration.
     * @param conf  Configuration containing MapReduce tool options.
     * @param str   Accumulo password string, used for input and/or output.
     */
    public static void setACPwd(Configuration conf, String str) {
        conf.set(AC_PWD_PROP, str);
    }

    /**
     * Sets the zookeepers in the given Configuration.
     * @param conf  Configuration containing MapReduce tool options.
     * @param str   List of zookeepers to use to connect to Accumulo.
     */
    public static void setACZK(Configuration conf, String str) {
        conf.set(AC_ZK_PROP, str);
    }

    /**
     * Sets the instance in the given Configuration.
     * @param conf  Configuration containing MapReduce tool options.
     * @param str   Accumulo instance name, for input and/or output.
     */
    public static void setACInstance(Configuration conf, String str) {
        conf.set(AC_INSTANCE_PROP, str);
    }

    /**
     * Sets the TTL in the given Configuration.
     * @param conf  Configuration containing MapReduce tool options.
     * @param str   TTL for Accumulo data. Rows older than this won't be scanned
     *              as input.
     */
    public static void setACTtl(Configuration conf, String str) {
        conf.set(AC_TTL_PROP, str);
    }

    /**
     * Sets whether to connect to a mock Accumulo instance.
     * @param conf  Configuration containing MapReduce tool options.
     * @param mock  true to use a mock instance, false to attempt to connect
     *              to a running Accumulo instance.
     */
    public static void setACMock(Configuration conf, boolean mock) {
        conf.setBoolean(AC_MOCK_PROP, mock);
    }

    /**
     * Sets the Rya table prefix in the given Configuration.
     * @param conf      Configuration containing MapReduce tool options.
     * @param prefix    Prefix of the Rya tables to use for input and/or output.
     */
    public static void setTablePrefix(Configuration conf, String prefix) {
        conf.set(TABLE_PREFIX_PROPERTY, prefix);
    }

    /**
     * Sets the table layout in the given Configuration.
     * @param conf  Configuration containing MapReduce tool options.
     * @param layout    The Rya core table to scan when using Rya for input.
     */
    public static void setTableLayout(Configuration conf, TABLE_LAYOUT layout) {
        conf.set(TABLE_LAYOUT_PROP, layout.toString());
    }

    /**
     * Sets the RDF serialization format in the given Configuration.
     * @param conf      Configuration containing MapReduce tool options.
     * @param format    The expected format of any RDF text data.
     */
    public static void setRDFFormat(Configuration conf, RDFFormat format) {
        conf.set(FORMAT_PROP, format.getName());
    }

    /**
     * Static class for accessing properties associated with Accumulo input
     * formats. Can allow input formats that don't extend
     * {@link InputFormatBase} to still use the same Accumulo input
     * configuration options.
     */
    @SuppressWarnings("rawtypes")
    public static class AccumuloProps extends InputFormatBase {
        /**
         * @throws UnsupportedOperationException always. This class should only be used to access properties.
         */
        @Override
        public RecordReader createRecordReader(InputSplit inputSplit, TaskAttemptContext taskAttemptContext) {
            throw new UnsupportedOperationException("Accumulo Props just holds properties");
        }
        public static Instance getInstance(JobContext  conf) {
            return InputFormatBase.getInstance(conf);
        }
        public static AuthenticationToken getPassword(JobContext  conf) {
            return InputFormatBase.getAuthenticationToken(conf);
        }
        public static String getUsername(JobContext conf) {
            return InputFormatBase.getPrincipal(conf);
        }
        public static String getTablename(JobContext conf) {
            return InputFormatBase.getInputTableName(conf);
        }
    }
}
