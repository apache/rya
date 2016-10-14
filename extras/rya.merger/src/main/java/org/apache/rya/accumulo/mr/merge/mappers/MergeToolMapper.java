package org.apache.rya.accumulo.mr.merge.mappers;

/*
 * #%L
 * org.apache.rya.accumulo.mr.merge
 * %%
 * Copyright (C) 2014 Rya
 * %%
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 * #L%
 */

import java.io.IOException;
import java.util.Collection;
import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.MutationsRejectedException;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.mapreduce.RangeInputSplit;
import org.apache.accumulo.core.data.ColumnUpdate;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.ColumnVisibility;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.log4j.Logger;

import com.google.common.base.Charsets;

import org.apache.rya.accumulo.AccumuloRdfConfiguration;
import org.apache.rya.accumulo.AccumuloRdfConstants;
import org.apache.rya.accumulo.AccumuloRyaDAO;
import org.apache.rya.accumulo.RyaTableMutationsFactory;
import org.apache.rya.accumulo.mr.MRUtils;
import org.apache.rya.accumulo.mr.merge.CopyTool;
import org.apache.rya.accumulo.mr.merge.MergeTool;
import org.apache.rya.accumulo.mr.merge.util.AccumuloRyaUtils;
import org.apache.rya.accumulo.mr.merge.util.TimeUtils;
import org.apache.rya.api.RdfCloudTripleStoreConfiguration;
import org.apache.rya.api.RdfCloudTripleStoreConstants;
import org.apache.rya.api.RdfCloudTripleStoreConstants.TABLE_LAYOUT;
import org.apache.rya.api.domain.RyaStatement;
import org.apache.rya.api.persist.RyaDAOException;
import org.apache.rya.api.resolver.RyaTripleContext;
import org.apache.rya.api.resolver.triple.TripleRow;
import org.apache.rya.api.resolver.triple.TripleRowResolverException;

/**
 * Reads from the Parent and Child tables comparing their the keys and adds or deletes the keys
 * from the parent as necessary in order to reflect changes that the child made since the provided
 * start time.
 */
public class MergeToolMapper extends Mapper<Key, Value, Text, Mutation> {
    private static final Logger log = Logger.getLogger(MergeToolMapper.class);

    private boolean usesStartTime;
    private String startTimeString;
    private Date startTime;
    private String parentTableName;
    private String childTableName;
    private String parentTablePrefix;
    private String childTablePrefix;
    private Text spoTable;
    private Text poTable;
    private Text ospTable;

    private Context context;

    private Configuration parentConfig;
    private Configuration childConfig;

    private AccumuloRdfConfiguration parentAccumuloRdfConfiguration;
    private AccumuloRdfConfiguration childAccumuloRdfConfiguration;

    private RyaTripleContext parentRyaContext;
    private RyaTripleContext childRyaContext;

    private RyaTableMutationsFactory ryaTableMutationFactory;

    private Scanner childScanner;
    private Iterator<Entry<Key, Value>> childIterator;
    private Connector childConnector;
    private AccumuloRyaDAO childDao;

    private Date copyToolInputTime;
    private Date copyToolRunTime;

    private Long parentTimeOffset = 0L;
    private Long childTimeOffset = 0L;

    private boolean useTimeSync = false;
    private boolean useMergeFileInput = false;

    /**
     * Creates a new {@link MergeToolMapper}.
     */
    public MergeToolMapper() {
    }

    /**
     * The result of comparing a child key and parent key which determines what should be done with them.
     */
    private static enum CompareKeysResult {
        /**
         * Indicates that the child iterator should move to the next key in the child
         * table in order to be compared to the current key in the parent table.
         */
        ADVANCE_CHILD,
        /**
         * Indicates that the child iterator should move to the next key in the child
         * table in order to be compared to the current key in the parent table
         * and that the current child key should be added to the parent.
         */
        ADVANCE_CHILD_AND_ADD,
        /**
         * Indicates that the parent iterator should move to the next key in the parent table
         * in order to be compared to the current key in the child table.
         */
        ADVANCE_PARENT,
        /**
         * Indicates that the parent iterator should move to the next key in the parent table
         * in order to be compared to the current key in the child table
         * and that the current parent key should be deleted from the parent.
         */
        ADVANCE_PARENT_AND_DELETE,
        /**
         * Indicates that the child iterator should move to the next key in the child table
         * and the parent iterator should move to the next key in the parent table.
         */
        ADVANCE_BOTH,
        /**
         * Indicates that there are no more keys to compare in the child and parent tables.
         */
        FINISHED;
    }

    /**
     * Expert users can override this method for more complete control over
     * the execution of the Mapper.
     *
     * @param context
     * @throws IOException
     */
    @Override
    public void run(Context context) throws IOException, InterruptedException {
        setup(context);
        this.context = context;

        try {
            RyaStatement parentRyaStatement = nextParentRyaStatement();
            RyaStatement childRyaStatement = nextChildRyaStatement();

            CompareKeysResult compareKeysResult = null;
            // Iteratively compare parent keys to child keys until finished
            while (compareKeysResult != CompareKeysResult.FINISHED) {
                compareKeysResult = compareKeys(parentRyaStatement, childRyaStatement);

                // Based on how the keys compare add or delete keys and advance the child or parent iterators forward
                switch (compareKeysResult) {
                    case ADVANCE_CHILD:
                        childRyaStatement = nextChildRyaStatement();
                        break;
                    case ADVANCE_PARENT:
                        parentRyaStatement = nextParentRyaStatement();
                        break;
                    case ADVANCE_CHILD_AND_ADD:
                        RyaStatement tempChildRyaStatement = childRyaStatement;
                        childRyaStatement = nextChildRyaStatement();
                        addKey(tempChildRyaStatement, context);
                        break;
                    case ADVANCE_PARENT_AND_DELETE:
                        RyaStatement tempParentRyaStatement = parentRyaStatement;
                        parentRyaStatement = nextParentRyaStatement();
                        deleteKey(tempParentRyaStatement, context);
                        break;
                    case ADVANCE_BOTH:
                        ColumnVisibility cv1 = new ColumnVisibility(parentRyaStatement.getColumnVisibility());
                        ColumnVisibility cv2 = new ColumnVisibility(childRyaStatement.getColumnVisibility());

                        // Update new column visibility now if necessary
                        if (!cv1.equals(cv2) && !cv2.equals(AccumuloRdfConstants.EMPTY_CV)) {
                            ColumnVisibility newCv = combineColumnVisibilities(cv1, cv2);
                            RyaStatement newCvRyaStatement = updateRyaStatementColumnVisibility(parentRyaStatement, newCv);

                            deleteKey(parentRyaStatement, context);
                            addKey(newCvRyaStatement, context);
                        }

                        parentRyaStatement = nextParentRyaStatement();
                        childRyaStatement = nextChildRyaStatement();
                        break;
                    case FINISHED:
                        log.info("Finished scanning parent and child tables");
                        break;
                    default:
                        log.error("Unknown result: " + compareKeysResult);
                        break;
                }
            }
        } catch (MutationsRejectedException | TripleRowResolverException e) {
            log.error("Error encountered while merging", e);
        } finally {
            cleanup(context);
        }
    }

    private RyaStatement nextParentRyaStatement() throws IOException, InterruptedException {
        return nextRyaStatement(context, parentRyaContext);
    }

    private RyaStatement nextChildRyaStatement() throws IOException, InterruptedException {
        return nextRyaStatement(childIterator, childRyaContext);
    }

    private static RyaStatement nextRyaStatement(Iterator<Entry<Key, Value>> iterator, RyaTripleContext ryaContext) {
        RyaStatement ryaStatement = null;
        if (iterator.hasNext()) {
            Entry<Key, Value> entry = iterator.next();
            Key key = entry.getKey();
            Value value = entry.getValue();
            try {
                ryaStatement = createRyaStatement(key, value, ryaContext);
            } catch (TripleRowResolverException e) {
                log.error("TripleRowResolverException encountered while creating statement", e);
            }
        }
        return ryaStatement;
    }

    private static RyaStatement nextRyaStatement(Context context, RyaTripleContext ryaContext) throws IOException, InterruptedException {
        RyaStatement ryaStatement = null;
        if (context.nextKeyValue()) {
            Key key = context.getCurrentKey();
            Value value = context.getCurrentValue();
            try {
                ryaStatement = createRyaStatement(key, value, ryaContext);
            } catch (TripleRowResolverException e) {
                log.error("TripleRowResolverException encountered while creating statement", e);
            }
        }
        return ryaStatement;
    }

    private static RyaStatement createRyaStatement(Key key, Value value, RyaTripleContext ryaTripleContext) throws TripleRowResolverException {
        byte[] row = key.getRowData() != null  && key.getRowData().toArray().length > 0 ? key.getRowData().toArray() : null;
        byte[] columnFamily = key.getColumnFamilyData() != null  && key.getColumnFamilyData().toArray().length > 0 ? key.getColumnFamilyData().toArray() : null;
        byte[] columnQualifier = key.getColumnQualifierData() != null  && key.getColumnQualifierData().toArray().length > 0 ? key.getColumnQualifierData().toArray() : null;
        Long timestamp = key.getTimestamp();
        byte[] columnVisibility = key.getColumnVisibilityData() != null && key.getColumnVisibilityData().toArray().length > 0 ? key.getColumnVisibilityData().toArray() : null;
        byte[] valueBytes = value != null && value.get().length > 0 ? value.get() : null;
        TripleRow tripleRow = new TripleRow(row, columnFamily, columnQualifier, timestamp, columnVisibility, valueBytes);
        RyaStatement ryaStatement = ryaTripleContext.deserializeTriple(TABLE_LAYOUT.SPO, tripleRow);

        return ryaStatement;
    }

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        super.setup(context);

        log.info("Setting up mapper");

        parentConfig = context.getConfiguration();
        childConfig = getChildConfig(parentConfig);

        startTimeString = parentConfig.get(MergeTool.START_TIME_PROP, null);
        if (startTimeString != null) {
            startTime = MergeTool.convertStartTimeStringToDate(startTimeString);
        }
        usesStartTime = startTime != null;
        useTimeSync = parentConfig.getBoolean(CopyTool.USE_NTP_SERVER_PROP, false);
        useMergeFileInput = parentConfig.getBoolean(MergeTool.USE_MERGE_FILE_INPUT, false);
        parentTableName = parentConfig.get(MergeTool.TABLE_NAME_PROP, null);
        parentTablePrefix = parentConfig.get(MRUtils.TABLE_PREFIX_PROPERTY, null);
        childTablePrefix = childConfig.get(MRUtils.TABLE_PREFIX_PROPERTY, null);
        if (useMergeFileInput) {
            childTableName = parentTableName.replaceFirst(parentTablePrefix, childTablePrefix) + MergeTool.TEMP_SUFFIX;
        } else {
            childTableName = parentTableName.replaceFirst(parentTablePrefix, childTablePrefix);
        }
        spoTable = new Text(parentTablePrefix + RdfCloudTripleStoreConstants.TBL_SPO_SUFFIX);
        poTable = new Text(parentTablePrefix + RdfCloudTripleStoreConstants.TBL_PO_SUFFIX);
        ospTable = new Text(parentTablePrefix + RdfCloudTripleStoreConstants.TBL_OSP_SUFFIX);

        childScanner = setupChildScanner(context);
        childIterator = childScanner.iterator();

        parentAccumuloRdfConfiguration = new AccumuloRdfConfiguration(parentConfig);
        parentAccumuloRdfConfiguration.setTablePrefix(parentTablePrefix);
        parentRyaContext = RyaTripleContext.getInstance(parentAccumuloRdfConfiguration);

        ryaTableMutationFactory = new RyaTableMutationsFactory(parentRyaContext);

        childAccumuloRdfConfiguration = new AccumuloRdfConfiguration(childConfig);
        childAccumuloRdfConfiguration.setTablePrefix(childTablePrefix);
        childRyaContext = RyaTripleContext.getInstance(childAccumuloRdfConfiguration);
        childConnector = AccumuloRyaUtils.setupConnector(childAccumuloRdfConfiguration);

        childDao = AccumuloRyaUtils.setupDao(childConnector, childAccumuloRdfConfiguration);

        if (startTime != null && useTimeSync) {
            try {
                copyToolInputTime = AccumuloRyaUtils.getCopyToolSplitDate(childDao);
                copyToolRunTime = AccumuloRyaUtils.getCopyToolRunDate(childDao);

                // Find the parent's time offset that was stored when the child was copied.
                parentTimeOffset = AccumuloRyaUtils.getTimeOffset(childDao);
                String durationBreakdown = TimeUtils.getDurationBreakdown(parentTimeOffset);
                log.info("The table " + parentTableName + " has a time offset of: " + durationBreakdown);
                childTimeOffset = Long.valueOf(childConfig.get(CopyTool.CHILD_TIME_OFFSET_PROP, null));
                Date adjustedParentStartTime = new Date(startTime.getTime() - parentTimeOffset);
                Date adjustedChildStartTime = new Date(startTime.getTime() - childTimeOffset);
                log.info("Adjusted parent start time: " + adjustedParentStartTime);
                log.info("Adjusted child start time: " + adjustedChildStartTime);
            } catch (RyaDAOException e) {
                log.error("Error getting time offset", e);
            }
        }

        log.info("Finished setting up mapper");
    }

    /**
     * Takes the child instance values in the configuration and puts into their corresponding parent instance values
     * so the config will connect to the child instance.
     * @param parentConfig the {@link Configuration} containing the parent and child properties.
     * @return the new {@link Configuration} where the parent connection values are replaced with
     * the child connection values.
     */
    public static Configuration getChildConfig(Configuration parentConfig) {
        Configuration childConfig = new Configuration(parentConfig);

        // Switch the temp child properties to be the main ones
        convertChildPropToParentProp(childConfig, parentConfig, MRUtils.AC_MOCK_PROP);
        convertChildPropToParentProp(childConfig, parentConfig, MRUtils.AC_INSTANCE_PROP);
        convertChildPropToParentProp(childConfig, parentConfig, MRUtils.AC_USERNAME_PROP);
        convertChildPropToParentProp(childConfig, parentConfig, MRUtils.AC_PWD_PROP);
        convertChildPropToParentProp(childConfig, parentConfig, MRUtils.TABLE_PREFIX_PROPERTY);
        convertChildPropToParentProp(childConfig, parentConfig, MRUtils.AC_AUTH_PROP);
        convertChildPropToParentProp(childConfig, parentConfig, RdfCloudTripleStoreConfiguration.CONF_QUERY_AUTH);
        convertChildPropToParentProp(childConfig, parentConfig, MRUtils.AC_ZK_PROP);

        MergeTool.setDuplicateKeys(childConfig);

        return childConfig;
    }

    /**
     * Looks for all properties in the parent/main configuration marked as a child value (by being appended with ".child")
     * and converts it in an unmarked property for the child config to use.
     * @param childConfig the child {@link Configuration}.
     * @param parentConfig the parent/main {@link Configuration}.
     * @param parentPropertyName the parent property name.
     */
    public static void convertChildPropToParentProp(Configuration childConfig, Configuration parentConfig, String parentPropertyName) {
        String childValue = parentConfig.get(parentPropertyName + MergeTool.CHILD_SUFFIX, "");
        childConfig.set(parentPropertyName, childValue);
    }

    /**
     * Combines 2 {@link ColumnVisibility ColumnVisibilities} by OR'ing them together.
     * @param cv1 the first (parent) {@link ColumnVisibility}.
     * @param cv2 the second (child) {@link ColumnVisibility}.
     * @return the newly combined {@link ColumnVisibility}.
     */
    public static ColumnVisibility combineColumnVisibilities(ColumnVisibility cv1, ColumnVisibility cv2) {
        // OR the 2 column visibilities together if they're different
        String columnVisibilityExpression;
        if (cv1.equals(AccumuloRdfConstants.EMPTY_CV)) {
            columnVisibilityExpression = new String(cv2.getExpression(), Charsets.UTF_8);
        } else {
            columnVisibilityExpression = "(" + new String(cv1.getExpression(), Charsets.UTF_8) + ")|("
                    + new String(cv2.getExpression(), Charsets.UTF_8) + ")";
        }
        ColumnVisibility newCv = new ColumnVisibility(new Text(columnVisibilityExpression));
        newCv = new ColumnVisibility(newCv.flatten());
        return newCv;
    }

    private Scanner setupChildScanner(Context context) throws IOException {
        return setupScanner(context, childTableName, childConfig);
    }

    private static Scanner setupScanner(Context context, String tableName, Configuration config) throws IOException {
        RangeInputSplit split = (RangeInputSplit) context.getInputSplit();
        Range splitRange = split.getRange();
        Scanner scanner = AccumuloRyaUtils.getScanner(tableName, config);
        scanner.setRange(splitRange);

        return scanner;
    }

    private void writeRyaMutations(RyaStatement ryaStatement, Context context, boolean isDelete) throws IOException, InterruptedException {
        if (ryaStatement.getColumnVisibility() == null) {
            ryaStatement.setColumnVisibility(AccumuloRdfConstants.EMPTY_CV.getExpression());
        }

        Map<TABLE_LAYOUT, Collection<Mutation>> mutationMap = ryaTableMutationFactory.serialize(ryaStatement);
        Collection<Mutation> spoMutations = mutationMap.get(TABLE_LAYOUT.SPO);
        Collection<Mutation> poMutations = mutationMap.get(TABLE_LAYOUT.PO);
        Collection<Mutation> ospMutations = mutationMap.get(TABLE_LAYOUT.OSP);

        for (Mutation mutation : spoMutations) {
            writeMutation(spoTable, mutation, context, isDelete);
        }
        for (Mutation mutation : poMutations) {
            writeMutation(poTable, mutation, context, isDelete);
        }
        for (Mutation mutation : ospMutations) {
            writeMutation(ospTable, mutation, context, isDelete);
        }
    }

    private void addKey(RyaStatement ryaStatement, Context context) throws IOException, InterruptedException {
        writeRyaMutations(ryaStatement, context, false);
    }

    private void deleteKey(RyaStatement ryaStatement, Context context) throws IOException, InterruptedException {
        writeRyaMutations(ryaStatement, context, true);
    }

    /**
     * Writes a mutation to the specified table.  If the mutation is meant to delete then the mutation will
     * be transformed to a delete mutation.
     * @param table the table to write to.
     * @param mutation the {@link mutation}.
     * @param context the {@link Context}.
     * @param isDelete {@code true} if the mutation should be a delete mutation.  {@code false} otherwise.
     * @throws IOException
     * @throws InterruptedException
     */
    private static void writeMutation(Text table, Mutation mutation, Context context, boolean isDelete) throws IOException, InterruptedException {
        if (isDelete) {
            List<ColumnUpdate> updates = mutation.getUpdates();
            ColumnUpdate columnUpdate = updates.get(0);
            ColumnVisibility cv = columnUpdate.getColumnVisibility() != null ? new ColumnVisibility(columnUpdate.getColumnVisibility()) : null;
            Mutation deleteMutation = new Mutation(new Text(mutation.getRow()));
            deleteMutation.putDelete(columnUpdate.getColumnFamily(), columnUpdate.getColumnQualifier(), cv, columnUpdate.getTimestamp());
            context.write(table, deleteMutation);
        } else {
            context.write(table, mutation);
        }
    }

    /**
     * Adjusts the date of a key's timestamp to account for the instance's machine local time offset.
     * @param date the timestamp {@link Date} to adjust.
     * @param isParentTable {@code true} if the timestamp is from a key in one of the parent instance's tables.
     * {@code false} if it's from the child instance.
     * @return the normalized {@link Date} or the same date if nothing needed to be adjusted.
     */
    private Date normalizeDate(Date date, boolean isParentTable) {
        Date normalizedDate = date;
        if (useTimeSync) {
            if (isParentTable) {
                normalizedDate = new Date(date.getTime() - parentTimeOffset);
            } else {
                // If the timestamp is before the time the child table was copied from
                // the parent then the timestamp originated from the parent machine
                if (TimeUtils.dateBeforeInclusive(date, copyToolRunTime)) {
                    normalizedDate = new Date(date.getTime() - parentTimeOffset);
                } else {
                    // Timestamps after the copy time originated from the child machine.
                    normalizedDate = new Date(date.getTime() - childTimeOffset);
                }
            }
        }
        return normalizedDate;
    }

    /**
     * Since both Scanners will return sorted data, if the two key-values are
     * equal, then both Scanners can advance to the next comparison. If the Key
     * from Scanner1 sorts before the Key from Scanner2, then that Key doesn't
     * exist in the table from Scanner2 which means Scanner1 should advance. If
     * the Key from Scanner2 sorts before the Key from Scanner1, then that Key
     * doesn't exist in the table from Scanner1 which means Scanner2 should
     * advance.
     * @param key1 the {@link RyaStatement} from the parent instance table.
     * @param key2 the {@link RyaStatement} from the child instance table.
     * @return the {@link CompareKeysResult}.
     * @throws MutationsRejectedException
     * @throws IOException
     * @throws InterruptedException
     * @throws TripleRowResolverException
     */
    private CompareKeysResult compareKeys(RyaStatement key1, RyaStatement key2) throws MutationsRejectedException, IOException, InterruptedException, TripleRowResolverException {
        log.trace("key1 = " + key1);
        log.trace("key2 = " + key2);
        if (key1 == null && key2 == null) {
            // Reached the end of the parent and child table.
            return CompareKeysResult.FINISHED;
        } else if (key1 == null) {
            // Reached the end of the parent table so add the remaining child keys if they meet the time criteria.
            Date t2 = normalizeDate(new Date(key2.getTimestamp()), false);
            // Move on to next comparison (do nothing) or add this child key to parent
            boolean doNothing = usesStartTime && t2.before(startTime);
            return doNothing ? CompareKeysResult.ADVANCE_CHILD : CompareKeysResult.ADVANCE_CHILD_AND_ADD;
        } else if (key2 == null) {
            // Reached the end of the child table so delete the remaining parent keys if they meet the time criteria.
            Date t1 = normalizeDate(new Date(key1.getTimestamp()), true);
            // Move on to next comparison (do nothing) or delete this key from parent
            boolean doNothing = usesStartTime && (copyToolInputTime != null && (t1.before(copyToolInputTime) || (t1.after(copyToolInputTime) && t1.after(startTime))) || (copyToolInputTime == null && t1.after(startTime)));
            return doNothing ? CompareKeysResult.ADVANCE_PARENT : CompareKeysResult.ADVANCE_PARENT_AND_DELETE;
        } else {
            // There are 2 keys to compare
            Map<TABLE_LAYOUT, TripleRow> map1 = parentRyaContext.serializeTriple(key1);
            Text row1 = new Text(map1.get(TABLE_LAYOUT.SPO).getRow());
            Map<TABLE_LAYOUT, TripleRow> map2 = childRyaContext.serializeTriple(key2);
            Text row2 = new Text(map2.get(TABLE_LAYOUT.SPO).getRow());
            Date t1 = normalizeDate(new Date(key1.getTimestamp()), true);
            Date t2 = normalizeDate(new Date(key2.getTimestamp()), false);

            if (row1.compareTo(row2) < 0) {
                // Parent key sort order was before the child key sort order
                // so it doesn't exist in the child table.
                // What does this mean?  Was it added by the parent after the child was cloned? (Meaning we should leave it)
                // Or did the child delete it after it was cloned? (Meaning we should delete it)
                boolean doNothing = usesStartTime && (copyToolInputTime != null && (t1.before(copyToolInputTime) || (t1.after(copyToolInputTime) && t1.after(startTime))) || (copyToolInputTime == null && t1.after(startTime)));
                return doNothing ? CompareKeysResult.ADVANCE_PARENT : CompareKeysResult.ADVANCE_PARENT_AND_DELETE;
            } else if (row1.compareTo(row2) > 0) {
                // Parent key sort order was after the child key sort order
                // so it doesn't exist in the parent table.
                // What does this mean?  Was it deleted by the parent after the child was cloned? (Meaning we should leave it)
                // Or did the child add it after it was cloned? (Meaning we should add it)
                boolean doNothing = usesStartTime && t2.before(startTime);
                return doNothing ? CompareKeysResult.ADVANCE_CHILD : CompareKeysResult.ADVANCE_CHILD_AND_ADD;
            } else {
                // Rows are the same. So just check if column visibility needs to be updated and
                // move on to the next parent and child keys.
                return CompareKeysResult.ADVANCE_BOTH;
            }
        }
    }

    private static RyaStatement updateRyaStatementColumnVisibility(RyaStatement ryaStatement, ColumnVisibility newCv) {
        RyaStatement newCvRyaStatement = new RyaStatement(ryaStatement.getSubject(), ryaStatement.getPredicate(), ryaStatement.getObject(), ryaStatement.getContext(), ryaStatement.getQualifer(), newCv.getExpression(), ryaStatement.getValue(), ryaStatement.getTimestamp());
        return newCvRyaStatement;
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        super.cleanup(context);
        log.info("Cleaning up mapper...");
        if (childScanner != null) {
            childScanner.close();
        }
        try {
            if (childDao != null) {
                childDao.destroy();
            }
        } catch (RyaDAOException e) {
            log.error("Error destroying child DAO", e);
        }
        log.info("Cleaned up mapper");
    }
}