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
package org.apache.rya.benchmark.periodic;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.time.LocalDateTime;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Arrays;
import java.util.List;
import java.util.LongSummaryStatistics;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import javax.xml.datatype.DatatypeConfigurationException;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.commons.io.FileUtils;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.rya.api.client.CreatePCJ.ExportStrategy;
import org.apache.rya.api.client.InstanceDoesNotExistException;
import org.apache.rya.api.client.LoadStatements;
import org.apache.rya.api.client.RyaClient;
import org.apache.rya.api.client.RyaClientException;
import org.apache.rya.api.model.VisibilityBindingSet;
import org.apache.rya.indexing.pcj.fluo.app.export.kafka.KryoVisibilityBindingSetSerializer;
import org.apache.rya.periodic.notification.serialization.BindingSetSerDe;
import org.openrdf.model.Statement;
import org.openrdf.query.BindingSet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.ParameterException;
import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

/**
 * This benchmark is useful for determining the performance characteristics of a Rya Triplestore under continuous ingest
 * that has a PCJ Query that is incrementally updated by the Rya PCJ Fluo App (aka PCJ Updater).
 * <p>
 * This benchmark periodically loads a batch of data into Rya and reports the delay of the following query
 *
 * <pre>
 * PREFIX time: &lt;http://www.w3.org/2006/time#&gt;
 * SELECT ?type (count(?obs) as ?total)
 * WHERE {
 *   ?obs &lt;uri:hasTime&gt; ?time .
 *   ?obs &lt;uri:hasObsType&gt; ?type .
 * }
 * GROUP BY ?type
 * </pre>
 * <p>
 * This benchmark is useful for characterizing any latency between Truth (data ingested to Rya) and Reported (query
 * result published to Kafka).
 * <p>
 * This benchmark is also useful for stress testing a Fluo App configuration and Accumulo Tablet configuration.
 * <p>
 * This benchmark expects the provided RyaInstance to have already been constructed and an appropriately configured Rya
 * PCJ Fluo App for that RyaInstance to be deployed on your YARN cluster.
 */
public class KafkaLatencyBenchmark implements AutoCloseable {

    public static final Logger logger = LoggerFactory.getLogger(KafkaLatencyBenchmark.class);

    /**
     * Data structure for storing Type
     */
    private final Map<String, Stat> typeToStatMap = Maps.newTreeMap();

    /**
     * ThreadPool for publishing data and logging stats.
     */
    private final ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(20);

    private final CommonOptions options;
    private final BenchmarkOptions benchmarkOptions;
    private final RyaClient client;
    DateTimeFormatter fsFormatter = DateTimeFormatter.ofPattern( "uuuu-MM-dd'T'HH-mm-ss" );
    private final LocalDateTime startTime;
    List<ScheduledFuture<?>> futureList = Lists.newArrayList();

    public KafkaLatencyBenchmark(final CommonOptions options, final BenchmarkOptions benchmarkOptions) throws AccumuloException, AccumuloSecurityException {
        this.options = Objects.requireNonNull(options);
        this.benchmarkOptions = Objects.requireNonNull(benchmarkOptions);
        this.client = Objects.requireNonNull(options.buildRyaClient());
        this.startTime = LocalDateTime.now();

        logger.info("Running {} with the following input parameters:\n{}\n{}", this.getClass(), options, benchmarkOptions);
    }

    @Override
    public void close() throws Exception {
        logger.info("Stopping threads.");
        scheduler.shutdown();

        cancelAllScheduledTasks();
        logger.info("Waiting for all threads to terminate...");
        scheduler.awaitTermination(1, TimeUnit.DAYS);
        logger.info("All threads terminated.");
    }

    private void cancelAllScheduledTasks() {
        logger.info("Canceling all tasks");
        for(final ScheduledFuture<?> task : futureList) {
            task.cancel(false);
        }
        futureList.clear();
    }


    public void start() throws InstanceDoesNotExistException, RyaClientException {
        logger.info("Issuing Query");
        String topic;
        boolean periodic;
        if(benchmarkOptions instanceof PeriodicQueryCommand) {
            topic = issuePeriodicQuery((PeriodicQueryCommand) benchmarkOptions);
            periodic = true;
        } else {
            topic = issueQuery();
            periodic = false;
        }
        logger.info("Query Issued. Received PCJ ID: {}", topic);
        startDataIngestTask();
        startStatsPrinterTask();
        startCsvPrinterTask();

        if(periodic) {
            updatePeriodicStatsFromKafka(topic);  // blocking operation.
        } else {
            updateStatsFromKafka(topic);  // blocking operation.
        }

    }

    private String issueQuery() throws InstanceDoesNotExistException, RyaClientException {
        final String sparql = "prefix time: <http://www.w3.org/2006/time#> "
                + "select ?type (count(?obs) as ?total) where { "
                + "    ?obs <uri:hasTime> ?time. "
                + "    ?obs <uri:hasObsType> ?type "
                + "} "
                + "group by ?type";

        logger.info("Query: {}", sparql);
        return client.getCreatePCJ().get().createPCJ(options.getRyaInstance(), sparql, ImmutableSet.of(ExportStrategy.KAFKA));
    }

    private String issuePeriodicQuery(final PeriodicQueryCommand periodicOptions) throws InstanceDoesNotExistException, RyaClientException {
        final String sparql = "prefix function: <http://org.apache.rya/function#> "
                + "prefix time: <http://www.w3.org/2006/time#> "
                + "select ?type (count(?obs) as ?total) where {"
                + "Filter(function:periodic(?time, " +  periodicOptions.getPeriodicQueryWindow() + ", " + periodicOptions.getPeriodicQueryPeriod() + ", time:" + periodicOptions.getPeriodicQueryTimeUnits() + ")) "
                + "?obs <uri:hasTime> ?time. "
                + "?obs <uri:hasObsType> ?type } "
                + "group by ?type";
        logger.info("Query: {}", sparql);
        final String queryId = client.getCreatePeriodicPCJ().get().createPeriodicPCJ(options.getRyaInstance(), sparql, periodicOptions.getPeriodicQueryRegistrationTopic(), options.getKafkaBootstrap());
        logger.info("Received query id: {}", queryId);
        return queryId.substring("QUERY_".length());  // remove the QUERY_ prefix.
    }


    private void startDataIngestTask() {
        final int initialPublishDelaySeconds = 1;

        final LoadStatements loadCommand = client.getLoadStatements();

        // initialize the stats map
        for(int typeId = 0; typeId < benchmarkOptions.getNumTypes(); typeId++) {
            final String type = benchmarkOptions.getTypePrefix() + typeId;
            typeToStatMap.put(type, new Stat(type));
        }

        final LoaderTask loaderTask = new LoaderTask(benchmarkOptions.getNumIterations(),
                benchmarkOptions.getObservationsPerTypePerIteration(), benchmarkOptions.getNumTypes(),
                benchmarkOptions.getTypePrefix(), loadCommand, options.getRyaInstance());

        final ScheduledFuture<?> loaderTaskFuture = scheduler.scheduleAtFixedRate(loaderTask, initialPublishDelaySeconds, benchmarkOptions.getIngestPeriodSeconds(), TimeUnit.SECONDS);
        futureList.add(loaderTaskFuture);

        loaderTask.setShutdownOperation(() -> {
            cancelAllScheduledTasks();
        });
    }

    private void startStatsPrinterTask() {
        final Runnable statLogger = () -> {
            final StringBuilder sb = new StringBuilder();
            sb.append("Results\n");
            for(final Stat s : typeToStatMap.values()) {
                sb.append(s).append("\n");
            }
            logger.info("{}",sb);
        };

        final int initialPrintDelaySeconds = 11;

        final ScheduledFuture<?> statLoggerFuture = scheduler.scheduleAtFixedRate(statLogger, initialPrintDelaySeconds, benchmarkOptions.getResultPeriodSeconds(), TimeUnit.SECONDS);
        futureList.add(statLoggerFuture);
    }

    private void startCsvPrinterTask() {

        final int initialPrintDelaySeconds = 11;
        final long printPeriodSeconds = benchmarkOptions.getResultPeriodSeconds();

        final Runnable csvPrinterTask = new Runnable() {
            private final AtomicInteger printCounter = new AtomicInteger(0);
            private final File outFile = new File(options.getOutputDirectory(), "run-" + fsFormatter.format(startTime) + ".csv");

            @Override
            public void run() {
                final int count = printCounter.getAndIncrement();
                final StringBuilder sb = new StringBuilder();
                if(count == 0) {
                    sb.append("elapsed-seconds");
                    for(final Stat s : typeToStatMap.values()) {
                      sb.append(",").append(s.getCsvStringHeader());
                    }
                    sb.append("\n");
                }

                sb.append(count*printPeriodSeconds);
                for(final Stat s : typeToStatMap.values()) {
                    sb.append(",").append(s.getCsvString());
                }
                sb.append("\n");
                try {
                    FileUtils.write(outFile, sb.toString(), StandardCharsets.UTF_8, true);
                } catch (final IOException e) {
                    logger.warn("Error writing to file " + outFile, e);
                }
            }
        };

        final ScheduledFuture<?> csvPrinterFuture = scheduler.scheduleAtFixedRate(csvPrinterTask, initialPrintDelaySeconds, printPeriodSeconds, TimeUnit.SECONDS);
        futureList.add(csvPrinterFuture);
    }



    private void updateStatsFromKafka(final String topic) {
        try (KafkaConsumer<String, VisibilityBindingSet> consumer = new KafkaConsumer<>(options.getKafkaConsumerProperties(), new StringDeserializer(), new KryoVisibilityBindingSetSerializer())) {
            consumer.subscribe(Arrays.asList(topic));
            while (!futureList.isEmpty()) {
                final ConsumerRecords<String, VisibilityBindingSet> records = consumer.poll(500);  // check kafka at most twice a second.
                handle(records);
            }
        } catch (final Exception e) {
            logger.warn("Exception occurred", e);
        }
    }

    private void updatePeriodicStatsFromKafka(final String topic) {
        try (KafkaConsumer<String, BindingSet> consumer = new KafkaConsumer<>(options.getKafkaConsumerProperties(), new StringDeserializer(), new BindingSetSerDe())) {
            consumer.subscribe(Arrays.asList(topic));
            while (!futureList.isEmpty()) {
                final ConsumerRecords<String, BindingSet> records = consumer.poll(500);  // check kafka at most twice a second.
                handle(records);
            }
        } catch (final Exception e) {
            logger.warn("Exception occurred", e);
        }
    }

    private void handle(final ConsumerRecords<String, ? extends BindingSet> records) {
        if(records.count() > 0) {
            logger.debug("Received {} records", records.count());
        }
        for(final ConsumerRecord<String, ? extends BindingSet> record: records){
            final BindingSet result = record.value();
            logger.debug("Received BindingSet: {}", result);

            final String type = result.getBinding("type").getValue().stringValue();
            final long total = Long.parseLong(result.getBinding("total").getValue().stringValue());

            final Stat stat = typeToStatMap.get(type);
            if(stat == null) {
                logger.warn("Not expecting to receive type: {}", type);
            } else {
                stat.fluoTotal.set(total);
            }
        }
    }

    private class LoaderTask implements Runnable {
        private final AtomicLong iterations = new AtomicLong(0);
        private final int numIterations;
        private final int numTypes;
        private final String typePrefix;
        private final long observationsPerTypePerIteration;

        private final LoadStatements loadStatements;
        private final String ryaInstanceName;
        private Runnable shutdownOperation;

        public LoaderTask(final int numIterations, final long observationsPerTypePerIteration, final int numTypes, final String typePrefix, final LoadStatements loadStatements, final String ryaInstanceName) {
            this.numIterations = numIterations;
            this.observationsPerTypePerIteration = observationsPerTypePerIteration;
            this.numTypes = numTypes;
            this.typePrefix = typePrefix;
            this.loadStatements = loadStatements;
            this.ryaInstanceName = ryaInstanceName;
        }

        @Override
        public void run() {
            try {
                final BenchmarkStatementGenerator gen = new BenchmarkStatementGenerator();

                final long i = iterations.getAndIncrement();
                logger.info("Publishing iteration [{} of {}]", i, numIterations);
                if(i >= numIterations) {
                    logger.info("Reached maximum iterations...");
                    shutdownOperation.run();
                    return;
                }
                final long observationsPerIteration = observationsPerTypePerIteration * numTypes;
                final long iterationOffset = i * observationsPerIteration;
                logger.info("Generating {} Observations", observationsPerIteration);
                final Iterable<Statement> statements = gen.generate(observationsPerTypePerIteration, numTypes, typePrefix, iterationOffset, ZonedDateTime.now());
                logger.info("Publishing {} Observations", observationsPerIteration);
                final long t1 = System.currentTimeMillis();
                loadStatements.loadStatements(ryaInstanceName, statements);
                logger.info("Published {} observations in in {}s", observationsPerIteration, ((System.currentTimeMillis() - t1)/1000.0));
                logger.info("Updating published totals...");
                for(int typeId = 0; typeId < numTypes; typeId++) {
                    typeToStatMap.get(typePrefix + typeId).total.addAndGet(observationsPerTypePerIteration);
                }
                logger.info("Finished publishing.");
            } catch (final RyaClientException e) {
                logger.warn("Error while writing statements", e);
            } catch (final DatatypeConfigurationException e) {
                logger.warn("Error creating generator", e);
            }

        }

        public void setShutdownOperation(final Runnable f) {
            this.shutdownOperation = f;
        }
    }


    /**
     * Simple data structure for storing and reporting statistics for a Type.
     */
    private class Stat {
        protected final AtomicLong fluoTotal = new AtomicLong(0);
        protected final AtomicLong total = new AtomicLong(0);
        private final String type;
        private final LongSummaryStatistics diffStats = new LongSummaryStatistics();
        public Stat(final String type) {
            this.type = type;
        }

        @Override
        public String toString() {
            final long t = total.get();
            final long ft = fluoTotal.get();
            final long diff = t - ft;
            diffStats.accept(diff);
            return type + " published total: " + t + " fluo: " + ft + " difference: " + diff + " diffStats: " + diffStats;
        }

        public String getCsvString() {
            final long t = total.get();
            final long ft = fluoTotal.get();
            final long diff = t - ft;
            return Joiner.on(",").join(type, t, ft, diff);
        }

        public String getCsvStringHeader() {
            return "type,published_total,fluo_total_" + type + ",difference_" + type;
        }
    }

    public static void main(final String[] args) {

        final CommonOptions options = new CommonOptions();
        final ProjectionQueryCommand projectionCommand = new ProjectionQueryCommand();
        final PeriodicQueryCommand periodicCommand = new PeriodicQueryCommand();

        BenchmarkOptions parsedCommand = null;
        final JCommander cli = new JCommander();
        cli.addObject(options);
        cli.addCommand(projectionCommand);
        cli.addCommand(periodicCommand);
        cli.setProgramName(KafkaLatencyBenchmark.class.getName());

        try {
            cli.parse(args);
            final String parsedName = cli.getParsedCommand();
            if ("periodic".equals(parsedName)) {
                parsedCommand = periodicCommand;
            }
            if ("projection".equals(parsedName)) {
                parsedCommand = projectionCommand;
            }
            if (parsedCommand == null) {
                throw new ParameterException("A command must be specified.");
            }
        } catch (final ParameterException e) {
            System.err.println("Error! Invalid input: " + e.getMessage());
            cli.usage();
            System.exit(1);
        }

        try (KafkaLatencyBenchmark benchmark = new KafkaLatencyBenchmark(options, parsedCommand)) {
            benchmark.start();
        } catch (final Exception e) {
          logger.warn("Exception occured.", e);
        }
    }
}
