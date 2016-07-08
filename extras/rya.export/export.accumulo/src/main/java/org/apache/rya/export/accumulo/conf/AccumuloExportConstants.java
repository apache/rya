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
package org.apache.rya.export.accumulo.conf;

import java.text.SimpleDateFormat;
import java.util.List;
import java.util.Map.Entry;

import org.apache.hadoop.conf.Configuration;
import org.apache.rya.export.accumulo.common.InstanceType;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

import mvm.rya.accumulo.mr.MRUtils;
import mvm.rya.api.RdfCloudTripleStoreConfiguration;
import mvm.rya.indexing.accumulo.ConfigUtils;

/**
 *
 */
public class AccumuloExportConstants {
    /**
     * Appended to certain config property names to indicate that the property is for the child instance.
     */
    public static final String CHILD_SUFFIX = ".child";

    /**
     * Suffix added to a child table when it is temporarily being imported into the parent instance when
     * being read from file and before the tables are merged together.
     */
    public static final String TEMP_SUFFIX = "_temp_child";

    /**
     * The time of the data to be included in the copy/merge process.
     */
    public static final String START_TIME_PROP = "tool.start.time";

    /**
     * The name of the table to process for the map reduce job.
     */
    public static final String TABLE_NAME_PROP = "tool.table.name";

    /**
     * "true" to use file input. "false" to use Accumulo output.
     */
    public static final String USE_MERGE_FILE_INPUT = "use.merge.file.input";

    /**
     * The file path to the child data input to merge in.
     */
    public static final String MERGE_FILE_INPUT_PATH = "merge.file.input.path";

    /**
     * Use this property to set the tables that are going to be copied.  The list should
     * be a comma-separated string containing the full table names.  If not set, then all
     * tables will be copied.
     */
    public static final String COPY_TABLE_LIST_PROP = "copy.table.list";

    /**
     * Indicates the type of child instance to create.  {@code null} or empty to not create an
     * instance indicating that it already was created and exists.
     */
    public static final String CREATE_CHILD_INSTANCE_TYPE_PROP = "create.child.instance.type";

    /**
     * The time difference between the parent machine and the time server.
     */
    public static final String PARENT_TIME_OFFSET_PROP = "time.offset";

    /**
     * The time difference between the child machine and the time server.
     */
    public static final String CHILD_TIME_OFFSET_PROP = "time.offset.child";

    /**
     * The host name of the time server to use.
     */
    public static final String NTP_SERVER_HOST_PROP = "ntp.server.host";

    /**
     * The URL of the Apache Tomcat server web page running on the parent machine.
     */
    public static final String PARENT_TOMCAT_URL_PROP = "tomcat.url";

    /**
     * The URL of the Apache Tomcat server web page running on the child machine.
     */
    public static final String CHILD_TOMCAT_URL_PROP = "tomcat.url.child";

    /**
     * The run time of the copy process.
     */
    public static final String COPY_RUN_TIME_PROP = "copy.run.time";

    /**
     * "true" to use the NTP server to handle time synchronization.
     * "false" (or any other value) to not use the NTP server.
     */
    public static final String USE_NTP_SERVER_PROP = "use.ntp.server";

    /**
     * "true" to use file output. "false" to use Accumulo output.
     */
    public static final String USE_COPY_FILE_OUTPUT = "use.copy.file.output";

    /**
     * The file path to output the child data to.
     */
    public static final String COPY_FILE_OUTPUT_PATH = "copy.file.output.path";

    /**
     * The compression type to use for file output.  One of "none", "gz", "lzo", or "snappy".
     */
    public static final String COPY_FILE_OUTPUT_COMPRESSION_TYPE = "copy.file.output.compression.type";

    /**
     * "true" to clear the file output directory before copying. "false" to leave the output directory alone.
     */
    public static final String USE_COPY_FILE_OUTPUT_DIRECTORY_CLEAR = "use.copy.file.output.directory.clear";

    /**
     * The input directory for importing files into accumulo tables.
     */
    public static final String COPY_FILE_IMPORT_DIRECTORY = "copy.file.import.directory";

    /**
     * "true" to read from the input directory. "false" otherwise.
     */
    public static final String USE_COPY_FILE_IMPORT = "use.copy.file.import";

    /**
     * "true" to extract a set of rules from a SPARQL query, and only copy statements relevant to those rules. "false" otherwise.
     * If set, either the query itself or a query file should also be provided.
     */
    public static final String USE_COPY_QUERY_SPARQL = "use.copy.query.sparql";

    /**
     * The text of the query that defines which statements to copy.
     */
    public static final String QUERY_STRING_PROP = "ac.copy.query";

    /**
     * The path to a file containing the query that defines which statements to copy.
     */
    public static final String QUERY_FILE_PROP = "ac.copy.queryfile";

    /**
     * The {@link InstanceType} to use for Accumulo.
     */
    public static final String ACCUMULO_INSTANCE_TYPE_PROP = "ac.instance.type";

    /**
     * A value used for the {@link #START_TIME_PROP} property to indicate that a dialog
     * should be displayed to select the time.
     */
    public static final String USE_START_TIME_DIALOG = "dialog";

    public static final SimpleDateFormat START_TIME_FORMATTER = new SimpleDateFormat("yyyyMMddHHmmssSSSz");

    /**
     * Map of keys that are supposed to use the same values.
     */
    public static final ImmutableMap<String, List<String>> DUPLICATE_KEY_MAP = ImmutableMap.<String, List<String>>builder()
        .put(MRUtils.AC_MOCK_PROP, ImmutableList.of(ConfigUtils.USE_MOCK_INSTANCE))
        .put(MRUtils.AC_INSTANCE_PROP, ImmutableList.of(ConfigUtils.CLOUDBASE_INSTANCE))
        .put(MRUtils.AC_USERNAME_PROP, ImmutableList.of(ConfigUtils.CLOUDBASE_USER))
        .put(MRUtils.AC_PWD_PROP, ImmutableList.of(ConfigUtils.CLOUDBASE_PASSWORD))
        .put(MRUtils.AC_AUTH_PROP, ImmutableList.of(ConfigUtils.CLOUDBASE_AUTHS, RdfCloudTripleStoreConfiguration.CONF_QUERY_AUTH))
        .put(MRUtils.AC_ZK_PROP, ImmutableList.of(ConfigUtils.CLOUDBASE_ZOOKEEPERS))
        .put(MRUtils.TABLE_PREFIX_PROPERTY, ImmutableList.of(ConfigUtils.CLOUDBASE_TBL_PREFIX, RdfCloudTripleStoreConfiguration.CONF_TBL_PREFIX))
        .put(MRUtils.AC_MOCK_PROP + CHILD_SUFFIX, ImmutableList.of(ConfigUtils.USE_MOCK_INSTANCE + CHILD_SUFFIX))
        .put(MRUtils.AC_INSTANCE_PROP + CHILD_SUFFIX, ImmutableList.of(ConfigUtils.CLOUDBASE_INSTANCE + CHILD_SUFFIX))
        .put(MRUtils.AC_USERNAME_PROP + CHILD_SUFFIX, ImmutableList.of(ConfigUtils.CLOUDBASE_USER + CHILD_SUFFIX))
        .put(MRUtils.AC_PWD_PROP + CHILD_SUFFIX, ImmutableList.of(ConfigUtils.CLOUDBASE_PASSWORD + CHILD_SUFFIX))
        .put(MRUtils.AC_AUTH_PROP + CHILD_SUFFIX, ImmutableList.of(ConfigUtils.CLOUDBASE_AUTHS + CHILD_SUFFIX, RdfCloudTripleStoreConfiguration.CONF_QUERY_AUTH + CHILD_SUFFIX))
        .put(MRUtils.AC_ZK_PROP + CHILD_SUFFIX, ImmutableList.of(ConfigUtils.CLOUDBASE_ZOOKEEPERS + CHILD_SUFFIX))
        .put(MRUtils.TABLE_PREFIX_PROPERTY + CHILD_SUFFIX, ImmutableList.of(ConfigUtils.CLOUDBASE_TBL_PREFIX + CHILD_SUFFIX, RdfCloudTripleStoreConfiguration.CONF_TBL_PREFIX + CHILD_SUFFIX))
        .build();

    /**
     * Sets duplicate keys in the config.
     * @param config the {@link Configuration}.
     */
    public static void setDuplicateKeys(final Configuration config) {
        for (final Entry<String, List<String>> entry : DUPLICATE_KEY_MAP.entrySet()) {
            final String key = entry.getKey();
            final List<String> duplicateKeys = entry.getValue();
            final String value = config.get(key);
            if (value != null) {
                for (final String duplicateKey : duplicateKeys) {
                    config.set(duplicateKey, value);
                }
            }
        }
    }

    /**
     * Sets all duplicate keys for the property in the config to the specified value.
     * @param config the {@link Configuration}.
     * @param property the property to set and all its duplicates.
     * @param value the value to set the property to.
     */
    public static void setDuplicateKeysForProperty(final Configuration config, final String property, final String value) {
        final List<String> duplicateKeys = DUPLICATE_KEY_MAP.get(property);
        config.set(property, value);
        if (duplicateKeys != null) {
            for (final String key : duplicateKeys) {
                config.set(key, value);
            }
        }
    }
}