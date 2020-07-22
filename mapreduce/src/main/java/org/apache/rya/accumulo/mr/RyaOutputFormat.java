package org.apache.rya.accumulo.mr;

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

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.BatchWriterConfig;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.MultiTableBatchWriter;
import org.apache.accumulo.core.client.MutationsRejectedException;
import org.apache.accumulo.core.client.mapreduce.AccumuloOutputFormat;
import org.apache.accumulo.core.data.Mutation;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.OutputCommitter;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.NullOutputFormat;
import org.apache.log4j.Logger;
import org.apache.rya.accumulo.AccumuloRdfConfiguration;
import org.apache.rya.accumulo.AccumuloRdfConstants;
import org.apache.rya.accumulo.AccumuloRyaDAO;
import org.apache.rya.api.RdfCloudTripleStoreConstants;
import org.apache.rya.api.domain.RyaIRI;
import org.apache.rya.api.domain.RyaResource;
import org.apache.rya.api.domain.RyaStatement;
import org.apache.rya.api.domain.RyaValue;
import org.apache.rya.api.persist.RyaDAOException;
import org.apache.rya.api.resolver.RdfToRyaConversions;
import org.apache.rya.api.resolver.RyaTripleContext;
import org.apache.rya.indexing.FreeTextIndexer;
import org.apache.rya.indexing.TemporalIndexer;
import org.apache.rya.indexing.accumulo.ConfigUtils;
import org.apache.rya.indexing.accumulo.entity.EntityCentricIndex;
import org.apache.rya.indexing.accumulo.freetext.AccumuloFreeTextIndexer;
import org.apache.rya.indexing.accumulo.temporal.AccumuloTemporalIndexer;
import org.eclipse.rdf4j.model.Statement;
import org.eclipse.rdf4j.model.vocabulary.XMLSchema;

import java.io.Closeable;
import java.io.Flushable;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.concurrent.TimeUnit;

/**
 * {@link OutputFormat} that uses Rya, the {@link GeoIndexer}, the
 * {@link FreeTextIndexer}, the {@link TemporalIndexer}, and the
 * {@link EntityCentricIndex} as the sink of triple data. This
 * OutputFormat ignores the Keys and only writes the Values to Rya.
 * <p>
 * The user must specify connection parameters for Rya, {@link GeoIndexer},
 * {@link FreeTextIndexer}, {@link TemporalIndexer}, and
 * {@link EntityCentricIndex}, if secondary indexing is desired.
 */
public class RyaOutputFormat extends OutputFormat<Writable, RyaStatementWritable> {
    private static final Logger logger = Logger.getLogger(RyaOutputFormat.class);

    private static final String PREFIX = RyaOutputFormat.class.getSimpleName();
    private static final String MAX_MUTATION_BUFFER_SIZE = PREFIX + ".maxmemory";
    private static final String ENABLE_FREETEXT = PREFIX + ".freetext.enable";
    private static final String ENABLE_TEMPORAL = PREFIX + ".temporal.enable";
    private static final String ENABLE_ENTITY = PREFIX + ".entity.enable";
    private static final String ENABLE_CORE = PREFIX + ".coretables.enable";
    private static final String OUTPUT_PREFIX_PROPERTY = PREFIX + ".tablePrefix";
    private static final String CV_PROPERTY = PREFIX + ".cv.default";
    private static final String CONTEXT_PROPERTY = PREFIX + ".context";

    /**
     * Set the default visibility of output: any statement whose visibility is
     * null will be written with this visibility instead. If not specified, use
     * an empty authorizations list.
     * @param job Job to apply the setting to.
     * @param visibility A comma-separated list of authorizations.
     */
    public static void setDefaultVisibility(final Job job, final String visibility) {
        if (visibility != null) {
            job.getConfiguration().set(CV_PROPERTY, visibility);
        }
    }

    /**
     * Set the default context (named graph) for any output: any statement whose
     * context is null will be written with this context instead. If not
     * specified, don't write any context.
     * @param job Job to apply the setting to.
     * @param context A context string, should be a syntactically valid URI.
     */
    public static void setDefaultContext(final Job job, final String context) {
        if (context != null) {
            job.getConfiguration().set(CONTEXT_PROPERTY, context);
        }
    }

    /**
     * Set the table prefix for output.
     * @param job Job to apply the setting to.
     * @param prefix The common prefix to all rya tables that output will be written to.
     */
    public static void setTablePrefix(final Job job, final String prefix) {
        job.getConfiguration().set(OUTPUT_PREFIX_PROPERTY, prefix);
    }

    /**
     * Set whether the free text index is enabled. Defaults to true.
     * @param job Job to apply the setting to.
     * @param enable Whether this job should add its output statements to the free text index.
     */
    public static void setFreeTextEnabled(final Job job, final boolean enable) {
        job.getConfiguration().setBoolean(ENABLE_FREETEXT, enable);
    }

     /**
     * Set whether the temporal index is enabled. Defaults to true.
     * @param job Job to apply the setting to.
     * @param enable Whether this job should add its output statements to the temporal index.
     */
    public static void setTemporalEnabled(final Job job, final boolean enable) {
        job.getConfiguration().setBoolean(ENABLE_TEMPORAL, enable);
    }

    /**
     * Set whether the entity-centric index is enabled. Defaults to true.
     * @param job Job to apply the setting to.
     * @param enable Whether this job should add its output statements to the entity-centric index.
     */
    public static void setEntityEnabled(final Job job, final boolean enable) {
        job.getConfiguration().setBoolean(ENABLE_ENTITY, enable);
    }

    /**
     * Set whether to insert to the core Rya tables (spo, osp, po). Defaults to true.
     * @param job Job to apply the setting to.
     * @param enable Whether this job should output to the core tables.
     */
    public static void setCoreTablesEnabled(final Job job, final boolean enable) {
        job.getConfiguration().setBoolean(ENABLE_CORE, enable);
    }

    /**
     * Configure a job to use a mock Accumulo instance.
     * @param job Job to configure
     * @param instance Name of the mock instance
     */
    public static void setMockInstance(final Job job, final String instance) {
        AccumuloOutputFormat.setMockInstance(job, instance);
        job.getConfiguration().setBoolean(ConfigUtils.USE_MOCK_INSTANCE, true);
        job.getConfiguration().setBoolean(MRUtils.AC_MOCK_PROP, true);
    }

    /**
     * Verify that all of the enabled indexers can be initialized.
     * @param   jobContext  Context containing configuration
     * @throws  IOException if initializing the core Rya indexer fails.
     */
    @Override
    public void checkOutputSpecs(final JobContext jobContext) throws IOException {
        final Configuration conf = jobContext.getConfiguration();
        // make sure that all of the indexers can connect
        getFreeTextIndexer(conf);
        getTemporalIndexer(conf);
        getRyaIndexer(conf);
    }

    /**
     * Get the OutputCommitter for this OutputFormat.
     * @param   context Context of the MapReduce task
     * @return  A committer whose method implementations are empty.
     */
    @Override
    public OutputCommitter getOutputCommitter(final TaskAttemptContext context) throws IOException, InterruptedException {
        // copied from AccumuloOutputFormat
        return new NullOutputFormat<Text, Mutation>().getOutputCommitter(context);
    }

    /**
     * Get this OutputFormat's RecordWriter.
     * @param   context     Context of the MapReduce task
     * @return  A RecordWriter that writes statements to Rya tables.
     * @throws  IOException if any enabled indexers can't be initialized
     */
    @Override
    public RecordWriter<Writable, RyaStatementWritable> getRecordWriter(final TaskAttemptContext context) throws IOException {
        return new RyaRecordWriter(context);
    }


    private static FreeTextIndexer getFreeTextIndexer(final Configuration conf) throws IOException {
        if (!conf.getBoolean(ENABLE_FREETEXT, true)) {
            return null;
        }
        final AccumuloFreeTextIndexer freeText = new AccumuloFreeTextIndexer();
        freeText.setConf(conf);
        Connector connector;
        try {
            connector = ConfigUtils.getConnector(conf);
        } catch (AccumuloException | AccumuloSecurityException e) {
            throw new IOException("Error when attempting to create a connection for writing the freeText index.", e);
        }
        final MultiTableBatchWriter mtbw = connector.createMultiTableBatchWriter(new BatchWriterConfig());
        freeText.setConnector(connector);
        freeText.setMultiTableBatchWriter(mtbw);
        freeText.init();

        return freeText;
    }

    private static TemporalIndexer getTemporalIndexer(final Configuration conf) throws IOException {
        if (!conf.getBoolean(ENABLE_TEMPORAL, true)) {
            return null;
        }
        final AccumuloTemporalIndexer temporal = new AccumuloTemporalIndexer();
        temporal.setConf(conf);
        Connector connector;
        try {
            connector = ConfigUtils.getConnector(conf);
        } catch (AccumuloException | AccumuloSecurityException e) {
            throw new IOException("Error when attempting to create a connection for writing the temporal index.", e);
        }
        final MultiTableBatchWriter mtbw = connector.createMultiTableBatchWriter(new BatchWriterConfig());
        temporal.setConnector(connector);
        temporal.setMultiTableBatchWriter(mtbw);
        temporal.init();
        return temporal;
    }

    private static EntityCentricIndex getEntityIndexer(final Configuration conf) {
        if (!conf.getBoolean(ENABLE_ENTITY, true)) {
            return null;
        }
        final EntityCentricIndex entity = new EntityCentricIndex();
        entity.setConf(conf);
        return entity;
    }

    private static AccumuloRyaDAO getRyaIndexer(final Configuration conf) throws IOException {
        try {
            if (!conf.getBoolean(ENABLE_CORE, true)) {
                return null;
            }
            final AccumuloRyaDAO ryaIndexer = new AccumuloRyaDAO();
            final Connector conn = ConfigUtils.getConnector(conf);
            ryaIndexer.setConnector(conn);

            final AccumuloRdfConfiguration ryaConf = new AccumuloRdfConfiguration();

            final String tablePrefix = conf.get(OUTPUT_PREFIX_PROPERTY, null);
            if (tablePrefix != null) {
                ryaConf.setTablePrefix(tablePrefix);
            }
            ryaConf.setDisplayQueryPlan(false);
            ryaIndexer.setConf(ryaConf);
            ryaIndexer.init();
            return ryaIndexer;
        } catch (final AccumuloException e) {
            logger.error("Cannot create RyaIndexer", e);
            throw new IOException(e);
        } catch (final AccumuloSecurityException e) {
            logger.error("Cannot create RyaIndexer", e);
            throw new IOException(e);
        } catch (final RyaDAOException e) {
            logger.error("Cannot create RyaIndexer", e);
            throw new IOException(e);
        }
    }

    /**
     * RecordWriter that takes in {@link RyaStatementWritable}s and writes them
     * to Rya tables.
     */
    public static class RyaRecordWriter extends RecordWriter<Writable, RyaStatementWritable>
            implements Closeable, Flushable {
        private static final Logger logger = Logger.getLogger(RyaRecordWriter.class);

        private final FreeTextIndexer freeTextIndexer;
        private final TemporalIndexer temporalIndexer;
        private final EntityCentricIndex entityIndexer;
        private final AccumuloRyaDAO ryaIndexer;
        private final RyaTripleContext tripleContext;
        private MultiTableBatchWriter writer;
        private byte[] cv = AccumuloRdfConstants.EMPTY_CV.getExpression();
        private RyaIRI defaultContext = null;

        private static final long ONE_MEGABYTE = 1024L * 1024L;
        private static final long AVE_STATEMENT_SIZE = 100L;

        private final long bufferSizeLimit;
        private long bufferCurrentSize = 0;

        private final ArrayList<RyaStatement> buffer;

        /**
         * Constructor.
         * @param context Context for MapReduce task
         * @throws  IOException if the core Rya indexer or entity indexer can't
         *          be initialized
         */
        public RyaRecordWriter(final TaskAttemptContext context) throws IOException {
            this(context.getConfiguration());
        }

        /**
         * Constructor.
         * @param conf Configuration containing any relevant options.
         * @throws  IOException if the core Rya indexer or entity indexer can't
         *          be initialized
         */
        public RyaRecordWriter(final Configuration conf) throws IOException {
            // set the visibility
            final String visibility = conf.get(CV_PROPERTY);
            if (visibility != null) {
                cv = visibility.getBytes(StandardCharsets.UTF_8);
            }
            // set the default context
            final String context = conf.get(CONTEXT_PROPERTY, "");
            if (context != null && !context.isEmpty()) {
                defaultContext = new RyaIRI(context);
            }

            // set up the buffer
            bufferSizeLimit = conf.getLong(MAX_MUTATION_BUFFER_SIZE, ONE_MEGABYTE);
            final int bufferCapacity = (int) (bufferSizeLimit / AVE_STATEMENT_SIZE);
            buffer = new ArrayList<RyaStatement>(bufferCapacity);

            // set up the indexers
            freeTextIndexer = getFreeTextIndexer(conf);
            temporalIndexer = getTemporalIndexer(conf);
            entityIndexer = getEntityIndexer(conf);
            ryaIndexer = getRyaIndexer(conf);

            // The entity index needs a batch writer -- typically it uses the DAO's, but decoupling
            // them lets it be used with or without the core tables, like the other indexers.
            if (entityIndexer != null) {
                Connector conn;
                try {
                    conn = ConfigUtils.getConnector(conf);
                } catch (AccumuloException | AccumuloSecurityException e) {
                    throw new IOException("Error connecting to Accumulo for entity index output", e);
                }
                final BatchWriterConfig batchWriterConfig = new BatchWriterConfig();
                batchWriterConfig.setMaxMemory(RdfCloudTripleStoreConstants.MAX_MEMORY);
                batchWriterConfig.setTimeout(RdfCloudTripleStoreConstants.MAX_TIME, TimeUnit.MILLISECONDS);
                batchWriterConfig.setMaxWriteThreads(RdfCloudTripleStoreConstants.NUM_THREADS);
                writer = conn.createMultiTableBatchWriter(batchWriterConfig);
                entityIndexer.setMultiTableBatchWriter(writer);
            }

            // update fields used for metrics
            startTime = System.currentTimeMillis();
            lastCommitFinishTime = startTime;

            // set up the triple context
            tripleContext = RyaTripleContext.getInstance(new AccumuloRdfConfiguration(conf));
        }

        /**
         * Write any buffered statements to Accumulo.
         * @throws IOException if any indexer can't be flushed.
         */
        @Override
        public void flush() throws IOException {
            flushBuffer();
        }

        /**
         * Close all indexers.
         */
        @Override
        public void close() {
            close(null);
        }

        /**
         * Close all indexers.
         * @param   paramTaskAttemptContext     Unused.
         */
        @Override
        public void close(final TaskAttemptContext paramTaskAttemptContext) {
            // close everything. log errors
            try {
                flush();
            } catch (final IOException e) {
                logger.error("Error flushing the buffer on RyaOutputFormat Close", e);
            }
            try {
                if (freeTextIndexer != null) {
                    freeTextIndexer.close();
                }
            } catch (final IOException e) {
                logger.error("Error closing the freetextIndexer on RyaOutputFormat Close", e);
            }
            try {
                if (temporalIndexer != null) {
                    temporalIndexer.close();
                }
            } catch (final IOException e) {
                logger.error("Error closing the temporalIndexer on RyaOutputFormat Close", e);
            }
            try {
                if (entityIndexer != null) {
                    entityIndexer.close();
                }
            } catch (final IOException e) {
                logger.error("Error closing the entityIndexer on RyaOutputFormat Close", e);
            }
            try {
                if (ryaIndexer != null) {
                    ryaIndexer.destroy();
                }
            } catch (final RyaDAOException e) {
                logger.error("Error closing RyaDAO on RyaOutputFormat Close", e);
            }
            if (writer != null) {
                try {
                    writer.close();
                } catch (final MutationsRejectedException e) {
                    logger.error("Error closing MultiTableBatchWriter on RyaOutputFormat Close", e);
                }
            }
        }

        /**
         * Write a {@link Statement} to Rya. Adds the statement to a buffer, and
         * flushes the statement buffer to the database if full.
         * @param   statement   Statement to insert to Rya.
         * @throws  IOException if writing to Accumulo fails.
         */
        public void write(final Statement statement) throws IOException {
            write(RdfToRyaConversions.convertStatement(statement));
        }

        /**
         * Writes a RyaStatement to Rya. Adds the statement to a buffer, and
         * flushes the statement buffer to the database if full.
         * @param   ryaStatement   Statement to insert to Rya.
         * @throws  IOException if writing to Accumulo fails.
         */
        public void write(final RyaStatement ryaStatement) throws IOException {
            write(NullWritable.get(), new RyaStatementWritable(ryaStatement, tripleContext));
        }

        /**
         * Writes a (key,value) pair to Rya. Adds the statement to a buffer, and
         * flushes the statement buffer to the database if full.
         * @param   key     Arbitrary Writable, not used.
         * @param   value   Contains statement to insert to Rya.
         * @throws  IOException if writing to Accumulo fails.
         */
        @Override
        public void write(final Writable key, final RyaStatementWritable value) throws IOException {
            final RyaStatement ryaStatement = value.getRyaStatement();
            if (ryaStatement.getColumnVisibility() == null) {
                ryaStatement.setColumnVisibility(cv);
            }
            if (ryaStatement.getContext() == null) {
                ryaStatement.setContext(defaultContext);
            }
            buffer.add(ryaStatement);
            bufferCurrentSize += statementSize(ryaStatement);
            if (bufferCurrentSize >= bufferSizeLimit) {
                flushBuffer();
            }
        }

        private int statementSize(final RyaStatement ryaStatement) {
            final RyaResource subject = ryaStatement.getSubject();
            final RyaIRI predicate = ryaStatement.getPredicate();
            final RyaValue object = ryaStatement.getObject();
            final RyaResource context = ryaStatement.getContext();
            int size = 3 + subject.getData().length() + predicate.getData().length() + object.getData().length();
            if (!XMLSchema.ANYURI.equals(object.getDataType())) {
                size += 2 + object.getDataType().toString().length();
            }
            if (context != null) {
                size += context.getData().length();
            }
            return size;
        }

        // fields for storing metrics
        private long startTime = 0;
        private long lastCommitFinishTime = 0;
        private long totalCommitRecords = 0;

        private double totalReadDuration = 0;
        private double totalWriteDuration = 0;

        private long commitCount = 0;

        private void flushBuffer() throws IOException {
            totalCommitRecords += buffer.size();
            commitCount++;

            final long startCommitTime = System.currentTimeMillis();

            logger.info(String.format("(C-%d) Flushing buffer with %,d objects and %,d bytes", commitCount, buffer.size(),
                    bufferCurrentSize));

            final double readingDuration = (startCommitTime - lastCommitFinishTime) / 1000.;
            totalReadDuration += readingDuration;
            final double currentReadRate = buffer.size() / readingDuration;
            final double totalReadRate = totalCommitRecords / totalReadDuration;

            // Print "reading" metrics
            logger.info(String.format("(C-%d) (Reading) Duration, Current Rate, Total Rate: %.2f %.2f %.2f ", commitCount, readingDuration,
                    currentReadRate, totalReadRate));

            // write to free text
            if (freeTextIndexer != null) {
                freeTextIndexer.storeStatements(buffer);
                freeTextIndexer.flush();
            }

            // write to temporal
            if (temporalIndexer != null) {
                temporalIndexer.storeStatements(buffer);
                temporalIndexer.flush();
            }

            // write to entity
            if (entityIndexer != null && writer != null) {
                entityIndexer.storeStatements(buffer);
                try {
                    writer.flush();
                } catch (final MutationsRejectedException e) {
                    throw new IOException("Error flushing data to Accumulo for entity indexing", e);
                }
            }

            // write to rya
            try {
                if (ryaIndexer != null) {
                    ryaIndexer.add(buffer.iterator());
                }
            } catch (final RyaDAOException e) {
                logger.error("Cannot write statement to Rya", e);
                throw new IOException(e);
            }

            lastCommitFinishTime = System.currentTimeMillis();

            final double writingDuration = (lastCommitFinishTime - startCommitTime) / 1000.;
            totalWriteDuration += writingDuration;
            final double currentWriteRate = buffer.size() / writingDuration;
            final double totalWriteRate = totalCommitRecords / totalWriteDuration;

            // Print "writing" stats
            logger.info(String.format("(C-%d) (Writing) Duration, Current Rate, Total Rate: %.2f %.2f %.2f ", commitCount, writingDuration,
                    currentWriteRate, totalWriteRate));

            final double processDuration = writingDuration + readingDuration;
            final double totalProcessDuration = totalWriteDuration + totalReadDuration;
            final double currentProcessRate = buffer.size() / processDuration;
            final double totalProcessRate = totalCommitRecords / (totalProcessDuration);

            // Print "total" stats
            logger.info(String.format("(C-%d) (Total) Duration, Current Rate, Total Rate: %.2f %.2f %.2f ", commitCount, processDuration,
                    currentProcessRate, totalProcessRate));

            // clear the buffer
            buffer.clear();
            bufferCurrentSize = 0L;
        }
    }
}
