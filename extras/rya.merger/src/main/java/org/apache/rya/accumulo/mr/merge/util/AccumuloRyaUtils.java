/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.rya.accumulo.mr.merge.util;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.Map.Entry;

import org.apache.accumulo.core.Constants;
import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.ClientConfiguration;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.Instance;
import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.client.ZooKeeperInstance;
import org.apache.accumulo.core.client.admin.SecurityOperations;
import org.apache.accumulo.core.client.mock.MockInstance;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.Filter;
import org.apache.accumulo.core.iterators.user.RegExFilter;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.log4j.Logger;
import org.openrdf.model.Literal;
import org.openrdf.model.ValueFactory;

import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableSet;

import info.aduna.iteration.CloseableIteration;
import org.apache.rya.accumulo.AccumuloRdfConfiguration;
import org.apache.rya.accumulo.AccumuloRyaDAO;
import org.apache.rya.accumulo.mr.MRUtils;
import org.apache.rya.api.RdfCloudTripleStoreConstants;
import org.apache.rya.api.domain.RyaStatement;
import org.apache.rya.api.domain.RyaType;
import org.apache.rya.api.domain.RyaURI;
import org.apache.rya.api.persist.RyaDAOException;
import org.apache.rya.api.resolver.RdfToRyaConversions;
import org.apache.rya.indexing.accumulo.ConfigUtils;

/**
 * Utility methods for an Accumulo Rya instance.
 */
public final class AccumuloRyaUtils {
    private static final Logger log = Logger.getLogger(AccumuloRyaUtils.class);

    public static final String COPY_TOOL_RUN_TIME_LOCAL_NAME = "copy_tool_run_time";
    public static final String COPY_TOOL_SPLIT_TIME_LOCAL_NAME = "copy_tool_split_time";
    public static final String COPY_TOOL_TIME_OFFSET_LOCAL_NAME = "copy_tool_time_offset";

    /**
     * ISO8601 time format.
     */
    private static final SimpleDateFormat TIME_FORMATTER = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSSX");
    private static final Date DEFAULT_DATE = new Date(0);

    private static final String NAMESPACE = RdfCloudTripleStoreConstants.NAMESPACE;
    private static final ValueFactory VALUE_FACTORY = RdfCloudTripleStoreConstants.VALUE_FACTORY;
    public static RyaURI RTS_SUBJECT_RYA = RdfCloudTripleStoreConstants.RTS_SUBJECT_RYA;
    public static RyaURI RTS_COPY_TOOL_RUN_TIME_PREDICATE_RYA = createRyaUri(COPY_TOOL_RUN_TIME_LOCAL_NAME);
    public static RyaURI RTS_COPY_TOOL_SPLIT_TIME_PREDICATE_RYA = createRyaUri(COPY_TOOL_SPLIT_TIME_LOCAL_NAME);
    public static RyaURI RTS_TIME_OFFSET_PREDICATE_RYA = createRyaUri(COPY_TOOL_TIME_OFFSET_LOCAL_NAME);

    /**
     * Ignore the meta statements indicating the Rya version and copy time values.
     */
    public static final ImmutableSet<IteratorSetting> COMMON_REG_EX_FILTER_SETTINGS = ImmutableSet.of(
        getVersionRegExFilterSetting(),
        getCopyToolRunTimeRegExFilterSetting(),
        getCopyToolSplitTimeRegExFilterSetting(),
        getCopyToolTimeOffsetRegExFilterSetting()
    );

    /**
     * Private constructor to prevent instantiation.
     */
    private AccumuloRyaUtils() {
    }

    /**
     * Creates a {@link RyaURI} for the specified local name.
     * @param localName the URI's local name.
     * @return the {@link RyraURI}.
     */
    public static RyaURI createRyaUri(final String localName) {
        return createRyaUri(NAMESPACE, localName);
    }

    /**
     * Creates a {@link RyaURI} for the specified local name.
     * @param namespace the namespace.
     * @param localName the URI's local name.
     * @return the {@link RyraURI}.
     */
    public static RyaURI createRyaUri(final String namespace, final String localName) {
        return RdfToRyaConversions.convertURI(VALUE_FACTORY.createURI(namespace, localName));
    }

    /**
     * Creates a copy tool run time {@link RyaStatement} from the specified {@link Date}.
     * @param date the copy tool run time {@link Date}.
     * @return the {@link RyaStatement} for the copy tool run time.
     */
    public static RyaStatement createCopyToolRunTimeRyaStatement(final Date date) {
        final Literal literal = VALUE_FACTORY.createLiteral(date != null ? date : DEFAULT_DATE);
        final RyaType timeObject = new RyaType(literal.getDatatype(), literal.stringValue());
        return new RyaStatement(RTS_SUBJECT_RYA, RTS_COPY_TOOL_RUN_TIME_PREDICATE_RYA, timeObject);
    }

    /**
     * Creates a copy tool split time {@link RyaStatement} from the specified {@link Date}.
     * @param date the copy tool split time {@link Date}.
     * @return the {@link RyaStatement} for the copy tool split time.
     */
    public static RyaStatement createCopyToolSplitTimeRyaStatement(final Date date) {
        final Literal literal = VALUE_FACTORY.createLiteral(date != null ? date : DEFAULT_DATE);
        final RyaType timeObject = new RyaType(literal.getDatatype(), literal.stringValue());
        return new RyaStatement(RTS_SUBJECT_RYA, RTS_COPY_TOOL_SPLIT_TIME_PREDICATE_RYA, timeObject);
    }

    /**
     * Gets the copy tool run {@link Date} metadata for the table.
     * @param dao the {@link AccumuloRyaDAO}.
     * @return the copy tool run {@link Date}.
     * @throws RyaDAOException
     */
    public static Date getCopyToolRunDate(final AccumuloRyaDAO dao) throws RyaDAOException {
        final String time = getCopyToolRunTime(dao);
        Date date = null;
        if (time != null) {
            try {
                date = TIME_FORMATTER.parse(time);
            } catch (final ParseException e) {
                log.error("Unable to parse the copy tool run time found in table: " + time, e);
            }
        }
        return date;
    }

    /**
     * Gets the copy tool split {@link Date} metadata for the table.
     * @param dao the {@link AccumuloRyaDAO}.
     * @return the copy tool split {@link Date}.
     * @throws RyaDAOException
     */
    public static Date getCopyToolSplitDate(final AccumuloRyaDAO dao) throws RyaDAOException {
        final String time = getCopyToolSplitTime(dao);
        Date date = null;
        if (time != null) {
            try {
                date = TIME_FORMATTER.parse(time);
            } catch (final ParseException e) {
                log.error("Unable to parse the copy tool split time found in table: " + time, e);
            }
        }
        return date;
    }

    /**
     * Gets the copy tool run time metadata for the table.
     * @param dao the {@link AccumuloRyaDAO}.
     * @return the copy tool run time value.
     * @throws RyaDAOException
     */
    public static String getCopyToolRunTime(final AccumuloRyaDAO dao) throws RyaDAOException {
        return getMetadata(RTS_COPY_TOOL_RUN_TIME_PREDICATE_RYA, dao);
    }

    /**
     * Gets the copy tool split time metadata for the table.
     * @param dao the {@link AccumuloRyaDAO}.
     * @return the copy tool split time value.
     * @throws RyaDAOException
     */
    public static String getCopyToolSplitTime(final AccumuloRyaDAO dao) throws RyaDAOException {
        return getMetadata(RTS_COPY_TOOL_SPLIT_TIME_PREDICATE_RYA, dao);
    }

    /**
     * Gets the metadata key from the table.
     * @param ryaStatement the {@link RyaStatement} for the metadata key to query.
     * @param dao the {@link AccumuloRyaDAO}.
     * @return the string value of the object from the metadata key.
     * @throws RyaDAOException
     */
    private static String getMetadata(final RyaURI predicateRyaUri, final AccumuloRyaDAO dao) throws RyaDAOException {
        final RyaStatement ryaStatement = new RyaStatement(RTS_SUBJECT_RYA, predicateRyaUri, null);
        return getMetadata(ryaStatement, dao);
    }

    /**
     * Gets the metadata key from the table.
     * @param ryaStatement the {@link RyaStatement} for the metadata key to query.
     * @param dao the {@link AccumuloRyaDAO}.
     * @return the string value of the object from the metadata key.
     * @throws RyaDAOException
     */
    private static String getMetadata(final RyaStatement ryaStatement, final AccumuloRyaDAO dao) throws RyaDAOException {
        String metadata = null;
        final AccumuloRdfConfiguration config = dao.getConf();
        final CloseableIteration<RyaStatement, RyaDAOException> iter = dao.getQueryEngine().query(ryaStatement, config);
        if (iter.hasNext()) {
            metadata = iter.next().getObject().getData();
        }
        iter.close();

        return metadata;
    }

    /**
     * Sets the copy tool run {@link Date} metadata key for the table.
     * @param date the copy tool run time {@link Date}.
     * @param dao the {@link AccumuloRyaDAO}.
     * @throws RyaDAOException
     */
    public static RyaStatement setCopyToolRunDate(final Date date, final AccumuloRyaDAO dao) throws RyaDAOException {
        final RyaStatement ryaStatement = createCopyToolRunTimeRyaStatement(date);
        dao.add(ryaStatement);
        return ryaStatement;
    }

    /**
     * Sets the copy tool split {@link Date} metadata key for the table.
     * @param date the tool copy split time {@link Date}.
     * @param dao the {@link AccumuloRyaDAO}.
     * @throws RyaDAOException
     */
    public static RyaStatement setCopyToolSplitDate(final Date date, final AccumuloRyaDAO dao) throws RyaDAOException {
        final RyaStatement ryaStatement = createCopyToolSplitTimeRyaStatement(date);
        dao.add(ryaStatement);
        return ryaStatement;
    }

    /**
     * Sets the copy tool run time metadata key for the table.
     * @param time the copy tool run time.
     * @param dao the {@link AccumuloRyaDAO}.
     * @throws RyaDAOException
     */
    public static RyaStatement setCopyToolRunTime(final String time, final AccumuloRyaDAO dao) throws RyaDAOException {
        Date date = null;
        try {
            date = TIME_FORMATTER.parse(time);
        } catch (final ParseException e) {
            log.error("Unable to parse the copy tool run time: ", e);
        }
        return setCopyToolRunDate(date, dao);
    }

    /**
     * Sets the copy tool split time metadata key for the table.
     * @param time the tool copy split time.
     * @param dao the {@link AccumuloRyaDAO}.
     * @throws RyaDAOException
     */
    public static RyaStatement setCopyToolSplitTime(final String time, final AccumuloRyaDAO dao) throws RyaDAOException {
        Date date = null;
        try {
            date = TIME_FORMATTER.parse(time);
        } catch (final ParseException e) {
            log.error("Unable to parse the copy tool split time: ", e);
        }
        return setCopyToolSplitDate(date, dao);
    }

    /**
     * Creates a {@link RegExFilter} setting to ignore the version row in a table.
     * @return the {@link RegExFilter} {@link IteratorSetting}.
     */
    public static IteratorSetting getVersionRegExFilterSetting() {
        final IteratorSetting regex = new IteratorSetting(30, "version_regex", RegExFilter.class);
        RegExFilter.setRegexs(regex, "(.*)urn:(.*)#version[\u0000|\u0001](.*)", null, null, null, false);
        Filter.setNegate(regex, true);
        return regex;
    }

    /**
     * Creates a {@link RegExFilter} setting to ignore the copy tool run time row in a table.
     * @return the {@link RegExFilter} {@link IteratorSetting}.
     */
    public static IteratorSetting getCopyToolRunTimeRegExFilterSetting() {
        final IteratorSetting regex = new IteratorSetting(31, COPY_TOOL_RUN_TIME_LOCAL_NAME + "_regex", RegExFilter.class);
        RegExFilter.setRegexs(regex, "(.*)urn:(.*)#" + COPY_TOOL_RUN_TIME_LOCAL_NAME + "[\u0000|\u0001](.*)", null, null, null, false);
        Filter.setNegate(regex, true);
        return regex;
    }

    /**
     * Creates a {@link RegExFilter} setting to ignore the copy tool split time row in a table.
     * @return the {@link RegExFilter} {@link IteratorSetting}.
     */
    public static IteratorSetting getCopyToolSplitTimeRegExFilterSetting() {
        final IteratorSetting regex = new IteratorSetting(32, COPY_TOOL_SPLIT_TIME_LOCAL_NAME + "_regex", RegExFilter.class);
        RegExFilter.setRegexs(regex, "(.*)urn:(.*)#" + COPY_TOOL_SPLIT_TIME_LOCAL_NAME + "[\u0000|\u0001](.*)", null, null, null, false);
        Filter.setNegate(regex, true);
        return regex;
    }

    /**
     * Creates a {@link RegExFilter} setting to ignore the copy tool time setting row in a table.
     * @return the {@link RegExFilter} {@link IteratorSetting}.
     */
    public static IteratorSetting getCopyToolTimeOffsetRegExFilterSetting() {
        final IteratorSetting regex = new IteratorSetting(33, COPY_TOOL_TIME_OFFSET_LOCAL_NAME + "_regex", RegExFilter.class);
        RegExFilter.setRegexs(regex, "(.*)urn:(.*)#" + COPY_TOOL_TIME_OFFSET_LOCAL_NAME + "[\u0000|\u0001](.*)", null, null, null, false);
        Filter.setNegate(regex, true);
        return regex;
    }

    /**
     * Adds all the common regex filter {@link IteratorSetting}s to the provided {@link Scanner} so
     * certain metadata keys in a table are ignored.
     * @param scanner the {@link Scanner} to add the regex filter {@link IteratorSetting}s to.
     */
    public static void addCommonScannerIteratorsTo(final Scanner scanner) {
        for (final IteratorSetting iteratorSetting : COMMON_REG_EX_FILTER_SETTINGS) {
            scanner.addScanIterator(iteratorSetting);
        }
    }

    /**
     * Creates a {@link Scanner} of the provided table name using the specified {@link Configuration}.
     * This applies common iterator settings to the table scanner that ignore internal metadata keys.
     * @param tablename the name of the table to scan.
     * @param config the {@link Configuration}.
     * @return the {@link Scanner} for the table.
     * @throws IOException
     */
    public static Scanner getScanner(final String tableName, final Configuration config) throws IOException {
        return getScanner(tableName, config, true);
    }

    /**
     * Creates a {@link Scanner} of the provided table name using the specified {@link Configuration}.
     * @param tablename the name of the table to scan.
     * @param config the {@link Configuration}.
     * @param shouldAddCommonIterators {@code true} to add the common iterators to the table scanner.
     * {@code false} otherwise.
     * @return the {@link Scanner} for the table.
     * @throws IOException
     */
    public static Scanner getScanner(final String tableName, final Configuration config, final boolean shouldAddCommonIterators) throws IOException {
        try {
            final String instanceName = config.get(ConfigUtils.CLOUDBASE_INSTANCE);
            final String zooKeepers = config.get(ConfigUtils.CLOUDBASE_ZOOKEEPERS);
            Instance instance;
            if (ConfigUtils.useMockInstance(config)) {
                instance = new MockInstance(config.get(ConfigUtils.CLOUDBASE_INSTANCE));
            } else {
                instance = new ZooKeeperInstance(new ClientConfiguration().withInstance(instanceName).withZkHosts(zooKeepers));
            }
            final String username = ConfigUtils.getUsername(config);
            final String password = ConfigUtils.getPassword(config);
            final Connector connector = instance.getConnector(username, new PasswordToken(password));
            final Authorizations auths = ConfigUtils.getAuthorizations(config);

            final Scanner scanner = connector.createScanner(tableName, auths);
            if (shouldAddCommonIterators) {
                AccumuloRyaUtils.addCommonScannerIteratorsTo(scanner);
            }
            return scanner;
        } catch (AccumuloException | AccumuloSecurityException | TableNotFoundException e) {
            log.error("Error connecting to " + tableName);
            throw new IOException(e);
        }
    }

    /**
     * Prints the table with the specified config and additional settings.
     * This applies common iterator settings to the table scanner that ignore internal metadata keys.
     * @param tableName the name of the table to print.
     * @param config the {@link AccumuloRdfConfiguration}.
     * @param settings the additional {@link IteratorSetting}s to add besides the common ones.
     * @throws IOException
     */
    public static void printTable(final String tableName, final AccumuloRdfConfiguration config, final IteratorSetting... settings) throws IOException {
        printTable(tableName, config, true, settings);
    }

    /**
     * Prints the table with the specified config and additional settings.
     * @param tableName the name of the table to print.
     * @param config the {@link AccumuloRdfConfiguration}.
     * @param shouldAddCommonIterators {@code true} to add the common iterators to the table scanner.
     * {@code false} otherwise.
     * @param settings the additional {@link IteratorSetting}s to add besides the common ones.
     * @throws IOException
     */
    public static void printTable(final String tableName, final AccumuloRdfConfiguration config, final boolean shouldAddCommonIterators, final IteratorSetting... settings) throws IOException {
        final Scanner scanner = AccumuloRyaUtils.getScanner(tableName, config, shouldAddCommonIterators);
        for (final IteratorSetting setting : settings) {
            scanner.addScanIterator(setting);
        }

        final Iterator<Entry<Key, Value>> iterator = scanner.iterator();

        final String instance = config.get(MRUtils.AC_INSTANCE_PROP);
        log.info("==================");
        log.info("TABLE: " + tableName + " INSTANCE: " + instance);
        log.info("------------------");
        while (iterator.hasNext()) {
            final Entry<Key, Value> entry = iterator.next();
            final Key key = entry.getKey();
            final Value value = entry.getValue();
            final String keyString = getFormattedKeyString(key);
            log.info(keyString + " - " + value);
        }
        log.info("==================");
    }

    private static String getFormattedKeyString(final Key key) {
        final StringBuilder sb = new StringBuilder();
        final byte[] row = key.getRow().getBytes();
        final byte[] colFamily = key.getColumnFamily().getBytes();
        final byte[] colQualifier = key.getColumnQualifier().getBytes();
        final byte[] colVisibility = key.getColumnVisibility().getBytes();
        final int maxRowDataToPrint = 256;
        Key.appendPrintableString(row, 0, row.length, maxRowDataToPrint, sb);
        sb.append(" ");
        Key.appendPrintableString(colFamily, 0, colFamily.length, maxRowDataToPrint, sb);
        sb.append(":");
        Key.appendPrintableString(colQualifier, 0, colQualifier.length, maxRowDataToPrint, sb);
        sb.append(" [");
        Key.appendPrintableString(colVisibility, 0, colVisibility.length, maxRowDataToPrint, sb);
        sb.append("]");
        sb.append(" ");
        sb.append(new Date(key.getTimestamp()));
        //sb.append(Long.toString(key.getTimestamp()));
        //sb.append(" ");
        //sb.append(key.isDeleted());
        return sb.toString();
    }

    /**
     * Prints the table with pretty formatting using the specified config and additional settings.
     * This applies common iterator settings to the table scanner that ignore internal metadata keys.
     * @param tableName the name of the table to print.
     * @param config the {@link AccumuloRdfConfiguration}.
     * @param settings the additional {@link IteratorSetting}s to add besides the common ones.
     * @throws IOException
     */
    public static void printTablePretty(final String tableName, final Configuration config, final IteratorSetting... settings) throws AccumuloException, AccumuloSecurityException, TableNotFoundException, IOException {
        printTablePretty(tableName, config, true, settings);
    }

    /**
     * Prints the table with pretty formatting using the specified config and additional settings.
     * @param tableName the name of the table to print.
     * @param config the {@link AccumuloRdfConfiguration}.
     * @param shouldAddCommonIterators {@code true} to add the common iterators to the table scanner.
     * {@code false} otherwise.
     * @param settings the additional {@link IteratorSetting}s to add besides the common ones.
     * @throws IOException
     */
    public static void printTablePretty(final String tableName, final Configuration config, final boolean shouldAddCommonIterators, final IteratorSetting... settings) throws AccumuloException, AccumuloSecurityException, TableNotFoundException, IOException {
        final Scanner scanner = AccumuloRyaUtils.getScanner(tableName, config, shouldAddCommonIterators);
        for (final IteratorSetting setting : settings) {
            scanner.addScanIterator(setting);
        }

        final String format = "| %-64s | %-24s | %-28s | %-20s | %-20s | %-10s |";
        final int totalFormatLength = String.format(format, 1, 2, 3, 4, 5, 6).length();
        final String instance = config.get(MRUtils.AC_INSTANCE_PROP);
        log.info(StringUtils.rightPad("==================", totalFormatLength, "="));
        log.info(StringUtils.rightPad("| TABLE: " + tableName + " INSTANCE: " + instance, totalFormatLength - 1) + "|");
        log.info(StringUtils.rightPad("------------------", totalFormatLength, "-"));
        log.info(String.format(format, "--Row--", "--ColumnVisibility--", "--Timestamp--", "--ColumnFamily--", "--ColumnQualifier--", "--Value--"));
        log.info(StringUtils.rightPad("|-----------------", totalFormatLength - 1, "-") + "|");
        for (final Entry<Key, Value> entry : scanner) {
            final Key k = entry.getKey();
            final String rowString = Key.appendPrintableString(k.getRow().getBytes(), 0, k.getRow().getLength(), Constants.MAX_DATA_TO_PRINT, new StringBuilder()).toString();
            log.info(String.format(format, rowString, k.getColumnVisibility(), new Date(k.getTimestamp()), k.getColumnFamily(), k.getColumnQualifier(), entry.getValue()));
        }
        log.info(StringUtils.rightPad("==================", totalFormatLength, "="));
    }

    /**
     * Adds authorizations to a user's authorizations list.
     * @param user the name of the user to add authorizations for.
     * @param secOps the {@link SecurityOperations}.
     * @param auths the {@link Authorizations} to add
     * @return the {@link Authorizations}.
     * @throws AccumuloException
     * @throws AccumuloSecurityException
     */
    public static Authorizations addUserAuths(final String user, final SecurityOperations secOps, final Authorizations auths) throws AccumuloException, AccumuloSecurityException {
        final List<String> authList = new ArrayList<>();
        for (final byte[] authBytes : auths.getAuthorizations()) {
            final String auth = new String(authBytes);
            authList.add(auth);
        }
        return addUserAuths(user, secOps, authList.toArray(new String[0]));
    }

    /**
     * Adds authorizations to a user's authorizations list.
     * @param user the name of the user to add authorizations for.
     * @param secOps the {@link SecurityOperations}.
     * @param auths the list of authorizations to add
     * @return the {@link Authorizations}.
     * @throws AccumuloException
     * @throws AccumuloSecurityException
     */
    public static Authorizations addUserAuths(final String user, final SecurityOperations secOps, final String... auths) throws AccumuloException, AccumuloSecurityException {
        final Authorizations currentUserAuths = secOps.getUserAuthorizations(user);
        final List<byte[]> authList = new ArrayList<>();
        for (final byte[] currentAuth : currentUserAuths.getAuthorizations()) {
            authList.add(currentAuth);
        }
        for (final String newAuth : auths) {
            authList.add(newAuth.getBytes());
        }
        final Authorizations result = new Authorizations(authList);
        return result;
    }

    /**
     * Removes the specified authorizations from the user.
     * @param userName the name of the user to change authorizations for.
     * @param secOps the {@link SecurityOperations} to change.
     * @param authsToRemove the comma-separated string of authorizations to remove.
     * @throws AccumuloSecurityException
     * @throws AccumuloException
     */
    public static void removeUserAuths(final String userName, final SecurityOperations secOps, final String authsToRemove) throws AccumuloException, AccumuloSecurityException {
        final Authorizations currentUserAuths = secOps.getUserAuthorizations(userName);
        final List<String> authList = convertAuthStringToList(currentUserAuths.toString());

        final List<String> authsToRemoveList = convertAuthStringToList(authsToRemove);
        authList.removeAll(authsToRemoveList);

        final String authString = Joiner.on(",").join(authList);
        final Authorizations newAuths = new Authorizations(authString);

        secOps.changeUserAuthorizations(userName, newAuths);
    }

    /**
     * Convert the comma-separated string of authorizations into a list of authorizations.
     * @param authString the comma-separated string of authorizations.
     * @return a {@link List} of authorization strings.
     */
    public static List<String> convertAuthStringToList(final String authString) {
        final List<String> authList = new ArrayList<>();
        if (authString != null) {
            final String[] authSplit = authString.split(",");
            authList.addAll(Arrays.asList(authSplit));
        }
        return authList;
    }

    /**
     * Sets up a {@link Connector} with the specified config.
     * @param accumuloRdfConfiguration the {@link AccumuloRdfConfiguration}.
     * @return the {@link Connector}.
     */
    public static Connector setupConnector(final AccumuloRdfConfiguration accumuloRdfConfiguration) {
        Connector connector = null;
        try {
            connector = ConfigUtils.getConnector(accumuloRdfConfiguration);
        } catch (AccumuloException | AccumuloSecurityException e) {
            log.error("Error creating connector", e);
        }

        return connector;
    }

    /**
     * Sets up a {@link AccumuloRyaDAO} with the specified connector.
     * @param connector the {@link Connector}.
     * @return the {@link AccumuloRyaDAO}.
     */
    public static AccumuloRyaDAO setupDao(final AccumuloRdfConfiguration accumuloRdfConfiguration) {
        final Connector connector = setupConnector(accumuloRdfConfiguration);
        return setupDao(connector, accumuloRdfConfiguration);
    }

    /**
     * Sets up a {@link AccumuloRyaDAO} with the specified connector and config.
     * @param connector the {@link Connector}.
     * @param accumuloRdfConfiguration the {@link AccumuloRdfConfiguration}.
     * @return the {@link AccumuloRyaDAO}.
     */
    public static AccumuloRyaDAO setupDao(final Connector connector, final AccumuloRdfConfiguration accumuloRdfConfiguration) {
        final AccumuloRyaDAO accumuloRyaDao = new AccumuloRyaDAO();
        accumuloRyaDao.setConnector(connector);
        accumuloRyaDao.setConf(accumuloRdfConfiguration);

        try {
            accumuloRyaDao.init();
        } catch (final RyaDAOException e) {
            log.error("Error initializing DAO", e);
        }

        return accumuloRyaDao;
    }

    /**
     * Creates a copy tool parent time offset {@link RyaStatement} from the specified offset.
     * @param timeOffset the copy tool parent time offset. (in milliseconds).
     * @return the {@link RyaStatement} for the copy tool parent time offset.
     */
    public static RyaStatement createTimeOffsetRyaStatement(final long timeOffset) {
        final Literal literal = VALUE_FACTORY.createLiteral(timeOffset);
        final RyaType timeObject = new RyaType(literal.getDatatype(), literal.stringValue());
        return new RyaStatement(RTS_SUBJECT_RYA, RTS_TIME_OFFSET_PREDICATE_RYA, timeObject);
    }

    /**
     * Gets the copy tool parent time offset metadata key for the table.
     * @param dao the {@link AccumuloRyaDAO}.
     * @return the difference between the parent machine's system time and
     * the NTP server's time or {@code null}.
     * @throws RyaDAOException
     */
    public static Long getTimeOffset(final AccumuloRyaDAO dao) throws RyaDAOException {
        final String timeOffsetString = getMetadata(RTS_TIME_OFFSET_PREDICATE_RYA, dao);
        Long timeOffset = null;
        if (timeOffsetString != null) {
            timeOffset = Long.valueOf(timeOffsetString);
        }
        return timeOffset;
    }

    /**
     * Sets the copy tool parent time offset metadata key for the table.
     * @param timeOffset the difference between the parent machine's system time and
     * the NTP server's time.
     * @param dao the {@link AccumuloRyaDAO}.
     * @throws RyaDAOException
     */
    public static RyaStatement setTimeOffset(final long timeOffset, final AccumuloRyaDAO dao) throws RyaDAOException {
        final RyaStatement ryaStatement = createTimeOffsetRyaStatement(timeOffset);
        dao.add(ryaStatement);
        return ryaStatement;
    }
}