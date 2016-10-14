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
package org.apache.rya.accumulo.mr.merge.mappers;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.TableExistsException;
import org.apache.accumulo.core.client.admin.SecurityOperations;
import org.apache.accumulo.core.client.mapreduce.lib.partition.KeyRangePartitioner;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.core.security.TablePermission;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.MRJobConfig;
import org.apache.hadoop.mapreduce.Mapper;

import com.google.common.base.Joiner;
import com.google.common.base.Splitter;
import com.google.common.collect.Lists;

import org.apache.rya.accumulo.AccumuloRdfConfiguration;
import org.apache.rya.accumulo.AccumuloRyaDAO;
import org.apache.rya.accumulo.mr.MRUtils;
import org.apache.rya.accumulo.mr.merge.CopyTool;
import org.apache.rya.accumulo.mr.merge.MergeTool;
import org.apache.rya.accumulo.mr.merge.util.AccumuloRyaUtils;
import org.apache.rya.api.persist.RyaDAOException;
import org.apache.rya.api.resolver.RyaTripleContext;
import twitter4j.Logger;

/**
 * The base {@link Mapper} for the copy tool which initializes the mapper for use.  The mapper will take all
 * keys from the parent table that are after the provided start time and copy them to the child table.
 */
public class BaseCopyToolMapper<KEYIN, VALUEIN, KEYOUT, VALUEOUT> extends Mapper<KEYIN, VALUEIN, KEYOUT, VALUEOUT> {
    private static final Logger log = Logger.getLogger(BaseCopyToolMapper.class);

    protected String startTimeString;
    protected Date startTime;
    protected Date runTime;
    protected Long timeOffset;
    protected boolean useCopyFileOutput;

    protected String parentTableName;
    protected String childTableName;
    protected String parentTablePrefix;
    protected String childTablePrefix;
    protected Text childTableNameText;

    protected Configuration parentConfig;
    protected Configuration childConfig;

    protected String parentUser;
    protected String childUser;

    protected Connector parentConnector;
    protected Connector childConnector;

    protected AccumuloRdfConfiguration parentAccumuloRdfConfiguration;
    protected AccumuloRdfConfiguration childAccumuloRdfConfiguration;

    protected RyaTripleContext childRyaContext;

    protected AccumuloRyaDAO childDao;

    /**
     * Creates a new {@link BaseCopyToolMapper}.
     */
    public BaseCopyToolMapper() {
    }

    @Override
    protected void setup(final Context context) throws IOException, InterruptedException {
        super.setup(context);

        log.info("Setting up mapper");

        parentConfig = context.getConfiguration();
        childConfig = MergeToolMapper.getChildConfig(parentConfig);

        startTimeString = parentConfig.get(MergeTool.START_TIME_PROP, null);
        if (startTimeString != null) {
            startTime = MergeTool.convertStartTimeStringToDate(startTimeString);
        }

        final String runTimeString = parentConfig.get(CopyTool.COPY_RUN_TIME_PROP, null);
        if (runTimeString != null) {
            runTime = MergeTool.convertStartTimeStringToDate(runTimeString);
        }

        final String offsetString = parentConfig.get(CopyTool.PARENT_TIME_OFFSET_PROP, null);
        if (offsetString != null) {
            timeOffset = Long.valueOf(offsetString);
        }

        useCopyFileOutput = parentConfig.getBoolean(CopyTool.USE_COPY_FILE_OUTPUT, false);

        parentTableName = parentConfig.get(MergeTool.TABLE_NAME_PROP, null);
        parentTablePrefix = parentConfig.get(MRUtils.TABLE_PREFIX_PROPERTY, null);
        childTablePrefix = childConfig.get(MRUtils.TABLE_PREFIX_PROPERTY, null);
        childTableName = parentTableName.replaceFirst(parentTablePrefix, childTablePrefix);
        childTableNameText = new Text(childTableName);
        log.info("Copying data from parent table, \"" + parentTableName + "\", to child table, \"" + childTableName + "\"");

        parentUser = parentConfig.get(MRUtils.AC_USERNAME_PROP, null);
        childUser = childConfig.get(MRUtils.AC_USERNAME_PROP, null);

        parentAccumuloRdfConfiguration = new AccumuloRdfConfiguration(parentConfig);
        parentAccumuloRdfConfiguration.setTablePrefix(parentTablePrefix);
        parentConnector = AccumuloRyaUtils.setupConnector(parentAccumuloRdfConfiguration);

        childAccumuloRdfConfiguration = new AccumuloRdfConfiguration(childConfig);
        childAccumuloRdfConfiguration.setTablePrefix(childTablePrefix);
        childRyaContext = RyaTripleContext.getInstance(childAccumuloRdfConfiguration);


        if (useCopyFileOutput) {
            fixSplitsInCachedLocalFiles();
        } else {
            childConnector = AccumuloRyaUtils.setupConnector(childAccumuloRdfConfiguration);
            childDao = AccumuloRyaUtils.setupDao(childConnector, childAccumuloRdfConfiguration);

            createTableIfNeeded();

            copyAuthorizations();
        }

        // Add the run time and split time to the table
        addMetadataKeys(context);

        log.info("Finished setting up mapper");
    }

    /**
     * Fixes the "splits.txt" file path in the "mapreduce.job.cache.local.files" property.  It contains the
     * {@link URI} "file:" prefix which causes {@link KeyRangePartitioner} to throw a {@code FileNotFoundException}
     * when it attempts to open it.
     */
    private void fixSplitsInCachedLocalFiles() {
        if (useCopyFileOutput) {
            // The "mapreduce.job.cache.local.files" property contains a comma-separated
            // list of cached local file paths.
            final String cachedLocalFiles = parentConfig.get(MRJobConfig.CACHE_LOCALFILES);
            if (cachedLocalFiles != null) {
                final List<String> cachedLocalFilesList = Lists.newArrayList(Splitter.on(',').split(cachedLocalFiles));
                final List<String> formattedCachedLocalFilesList = new ArrayList<>();
                for (final String cachedLocalFile : cachedLocalFilesList) {
                    String pathToAdd = cachedLocalFile;
                    if (cachedLocalFile.endsWith("splits.txt")) {
                        URI uri = null;
                        try {
                            uri = new URI(cachedLocalFiles);
                            pathToAdd = uri.getPath();
                        } catch (final URISyntaxException e) {
                            log.error("Invalid syntax in local cache file path", e);
                        }
                    }
                    formattedCachedLocalFilesList.add(pathToAdd);
                }
                final String formattedCachedLocalFiles = Joiner.on(',').join(formattedCachedLocalFilesList);
                if (!cachedLocalFiles.equals(formattedCachedLocalFiles)) {
                    parentConfig.set(MRJobConfig.CACHE_LOCALFILES, formattedCachedLocalFiles);
                }
            }
        }
    }

    protected void addMetadataKeys(final Context context) throws IOException {
        try {
            if (AccumuloRyaUtils.getCopyToolRunDate(childDao) == null) {
                log.info("Writing copy tool run time metadata to child table: " + runTime);
                AccumuloRyaUtils.setCopyToolRunDate(runTime, childDao);
            }
            if (AccumuloRyaUtils.getCopyToolSplitDate(childDao) == null) {
                log.info("Writing copy split time metadata to child table: " + startTime);
                AccumuloRyaUtils.setCopyToolSplitDate(startTime, childDao);
            }

            if (timeOffset != null) {
                log.info("Writing copy tool time offset metadata to child table: " + timeOffset);
                AccumuloRyaUtils.setTimeOffset(timeOffset, childDao);
            }
        } catch (final RyaDAOException e) {
            throw new IOException("Failed to set time metadata key for table: " + childTableName, e);
        }
    }

    private void createTableIfNeeded() throws IOException {
        try {
            if (!childConnector.tableOperations().exists(childTableName)) {
                log.info("Creating table: " + childTableName);
                childConnector.tableOperations().create(childTableName);
                log.info("Created table: " + childTableName);
                log.info("Granting authorizations to table: " + childTableName);
                childConnector.securityOperations().grantTablePermission(childUser, childTableName, TablePermission.WRITE);
                log.info("Granted authorizations to table: " + childTableName);
            }
        } catch (TableExistsException | AccumuloException | AccumuloSecurityException e) {
            throw new IOException(e);
        }
    }

    protected void copyAuthorizations() throws IOException {
        try {
            final SecurityOperations parentSecOps = parentConnector.securityOperations();
            final SecurityOperations childSecOps = childConnector.securityOperations();

            final Authorizations parentAuths = parentSecOps.getUserAuthorizations(parentUser);
            final Authorizations childAuths = childSecOps.getUserAuthorizations(childUser);
            // Add any parent authorizations that the child doesn't have.
            if (!childAuths.equals(parentAuths)) {
                log.info("Adding the authorization, \"" + parentAuths.toString() + "\", to the child user, \"" + childUser + "\"");
                final Authorizations newChildAuths = AccumuloRyaUtils.addUserAuths(childUser, childSecOps, parentAuths);
                childSecOps.changeUserAuthorizations(childUser, newChildAuths);
            }
        } catch (AccumuloException | AccumuloSecurityException e) {
            throw new IOException(e);
        }
    }

    @Override
    protected void cleanup(final Context context) throws IOException, InterruptedException {
        super.cleanup(context);
        log.info("Cleaning up mapper...");
        try {
            if (childDao != null) {
                childDao.destroy();
            }
        } catch (final RyaDAOException e) {
            log.error("Error destroying child DAO", e);
        }
        log.info("Cleaned up mapper");
    }
}