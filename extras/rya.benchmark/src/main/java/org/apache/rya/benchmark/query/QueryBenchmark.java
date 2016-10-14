/**
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
package org.apache.rya.benchmark.query;

import static java.util.Objects.requireNonNull;

import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

import javax.annotation.ParametersAreNonnullByDefault;

import org.apache.log4j.ConsoleAppender;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.log4j.PatternLayout;
import org.apache.rya.benchmark.query.Parameters.NumReadsRuns;
import org.apache.rya.benchmark.query.QueryBenchmark.QueryBenchmarkRun.NotEnoughResultsException;
import org.apache.rya.benchmark.query.Rya.Accumulo;
import org.apache.rya.benchmark.query.Rya.SecondaryIndexing;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Timeout;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.options.CommandLineOptions;
import org.openjdk.jmh.runner.options.OptionsBuilder;
import org.openrdf.query.BindingSet;
import org.openrdf.query.MalformedQueryException;
import org.openrdf.query.QueryEvaluationException;
import org.openrdf.query.parser.ParsedQuery;
import org.openrdf.query.parser.sparql.SPARQLParser;
import org.openrdf.sail.Sail;
import org.openrdf.sail.SailConnection;
import org.openrdf.sail.SailException;

import info.aduna.iteration.CloseableIteration;
import org.apache.rya.accumulo.AccumuloRdfConfiguration;
import org.apache.rya.indexing.accumulo.ConfigUtils;
import org.apache.rya.indexing.external.PrecomputedJoinIndexerConfig.PrecomputedJoinStorageType;
import org.apache.rya.indexing.external.PrecomputedJoinIndexerConfig.PrecomputedJoinUpdaterType;
import org.apache.rya.sail.config.RyaSailFactory;

/**
 * A benchmark that may be used to evaluate the performance of SPARQL queries
 * over a living instance of Rya. It pivots over two dimensions:
 * <ul>
 *     <li>Which SPARQL query to execute</li>
 *     <li>How many of the query's results to read</li>
 * </ul>
 * </p>
 * These parameters are configured by placing a file named "queries-benchmark-conf.xml"
 * within the directory the benchmark is being executed from. The schema that defines
 * this XML file is named "queries-benchmark-conf.xsd" and may be found embedded within
 * the benchmark's jar file.
 * </p>
 * To execute this benchmark, build the project by executing:
 * <pre>
 * mvn clean install
 * </pre>
 * Transport the "target/benchmarking.jar" file to the system that will execute
 * the benchmark, write the configuration file, and then execute:
 * <pre>
 * java -cp benchmarks.jar org.apache.rya.benchmark.query.QueryBenchmark
 * </pre>
 */
@State(Scope.Thread)
public class QueryBenchmark {

    /**
     * The path to the configuration file that this benchmark uses to connect to Rya.
     */
    public static final Path QUERY_BENCHMARK_CONFIGURATION_FILE = Paths.get("queries-benchmark-conf.xml");

    /**
     * Indicates all query results will be read during the benchmark.
     */
    public static final String READ_ALL = "ALL";

    @Param({"1", "10", "100", READ_ALL})
    public String numReads;

    @Param({})
    public String sparql;

    private Sail sail = null;
    private SailConnection sailConn = null;

    @Setup
    public void setup() throws Exception {
        // Setup logging.
        final ConsoleAppender console = new ConsoleAppender();
        console.setLayout(new PatternLayout("%d [%p|%c|%C{1}] %m%n"));
        console.setThreshold(Level.INFO);
        console.activateOptions();
        Logger.getRootLogger().addAppender(console);

        // Load the benchmark's configuration file.
        final InputStream queriesStream = Files.newInputStream(QUERY_BENCHMARK_CONFIGURATION_FILE);
        final QueriesBenchmarkConf benchmarkConf = new QueriesBenchmarkConfReader().load(queriesStream);

        // Create the Rya Configuration object using the benchmark's configuration.
        final AccumuloRdfConfiguration ryaConf = new AccumuloRdfConfiguration();

        final Rya rya = benchmarkConf.getRya();
        ryaConf.setTablePrefix(rya.getRyaInstanceName());

        final Accumulo accumulo = rya.getAccumulo();
        ryaConf.set(ConfigUtils.CLOUDBASE_USER, accumulo.getUsername());
        ryaConf.set(ConfigUtils.CLOUDBASE_PASSWORD, accumulo.getPassword());
        ryaConf.set(ConfigUtils.CLOUDBASE_ZOOKEEPERS, accumulo.getZookeepers());
        ryaConf.set(ConfigUtils.CLOUDBASE_INSTANCE, accumulo.getInstanceName());

        // Print the query plan so that you can visually inspect how PCJs are being applied for each benchmark.
        ryaConf.set(ConfigUtils.DISPLAY_QUERY_PLAN, "true");

        // Turn on PCJs if we are configured to use them.
        final SecondaryIndexing secondaryIndexing = rya.getSecondaryIndexing();
        if(secondaryIndexing.isUsePCJ()) {
            ryaConf.set(ConfigUtils.USE_PCJ, "true");
            ryaConf.set(ConfigUtils.PCJ_STORAGE_TYPE, PrecomputedJoinStorageType.ACCUMULO.toString());
            ryaConf.set(ConfigUtils.PCJ_UPDATER_TYPE, PrecomputedJoinUpdaterType.NO_UPDATE.toString());
        } else {
            ryaConf.set(ConfigUtils.USE_PCJ, "false");
        }

        // Create the connections used to execute the benchmark.
        sail = RyaSailFactory.getInstance( ryaConf );
        sailConn = sail.getConnection();
    }

    @TearDown
    public void tearDown() {
        try {
            sailConn.close();
        } catch(final Exception e) { }

        try {
            sail.shutDown();
        } catch(final Exception e) { }
    }

    @Benchmark
    @BenchmarkMode(Mode.SingleShotTime)
    @Timeout(time = 1, timeUnit = TimeUnit.HOURS)
    public void queryRya() throws MalformedQueryException, QueryEvaluationException, SailException, NotEnoughResultsException {
        final QueryBenchmarkRun benchmark;

        if(numReads.equals( READ_ALL )) {
            benchmark = new QueryBenchmarkRun(sailConn, sparql);
        } else {
            benchmark = new QueryBenchmarkRun(sailConn, sparql, Long.parseLong(numReads));
        }

        benchmark.run();
    }

    /**
     * Runs the query benchmarks.
     * </p>
     * Example command line:
     * <pre>
     * java -cp benchmarks.jar org.apache.rya.benchmark.query.QueryBenchmark
     * </pre>
     *
     * @param args - The command line arguments that will be fed into the benchmark.
     * @throws Exception The benchmark could not be run.
     */
    public static void main(final String[] args) throws Exception {
        // Read the queries that will be benchmarked from the provided path.
        final InputStream queriesStream = Files.newInputStream( QUERY_BENCHMARK_CONFIGURATION_FILE );
        final QueriesBenchmarkConf benchmarkConf = new QueriesBenchmarkConfReader().load(queriesStream);
        final Parameters parameters = benchmarkConf.getParameters();

        // Setup the options that will be used to run the benchmark.
        final OptionsBuilder options = new OptionsBuilder();
        options.parent( new CommandLineOptions(args) );
        options.include(QueryBenchmark.class.getSimpleName());

        // Provide the SPARQL queries that will be injected into the benchmark's 'sparql' parameter.
        final List<String> sparql = parameters.getQueries().getSPARQL();
        final String[] sparqlArray = new String[ sparql.size() ];
        sparql.toArray( sparqlArray );

        // Clean up the sparql's whitespace.
        for(int i = 0; i < sparqlArray.length; i++) {
            sparqlArray[i] = sparqlArray[i].trim();
        }

        options.param("sparql", sparqlArray);

        // If numReadsRuns was specified, inject them into the benchmark's 'numReads' parameter.
        final NumReadsRuns numReadsRuns = parameters.getNumReadsRuns();
        if(numReadsRuns != null) {
            // Validate the list.
            final List<String> numReadsList = numReadsRuns.getNumReads();
            for(final String numReads : numReadsList) {
                // It may be the READ_ALL flag.
                if(numReads.equals(READ_ALL)) {
                    continue;
                }

                // Or it must be a Long.
                try {
                    Long.parseLong(numReads);
                } catch(final NumberFormatException e) {
                    throw new RuntimeException("There is a problem with the benchmark's configuration. Encountered " +
                            "a numReads value of '" + numReads + "', which is inavlid. The value must be a Long or " +
                            "'" + READ_ALL + "'");
                }
            }

            // Configure the benchmark with the numRuns.
            final String[] numReadsArray = new String[ numReadsList.size() ];
            numReadsList.toArray( numReadsArray );
            options.param("numReads", numReadsArray);
        }

        // Execute the benchmark.
        new Runner(options.build()).run();
    }

    /**
     * Executes an iteration of the benchmarked logic.
     */
    @ParametersAreNonnullByDefault
    public static final class QueryBenchmarkRun {

        private final SailConnection sailConn;
        private final String sparql;
        private final Optional<Long> numReads;

        /**
         * Constructs an instance of {@link QueryBenchmarkRun} that will read all of the results of the query.
         *
         * @param sailConn - The connection to the Rya instance the query will be executed against. (not null)
         * @param sparql - The query that will be executed. (not null)
         */
        public QueryBenchmarkRun(final SailConnection sailConn, final String sparql) {
            this.sailConn = requireNonNull(sailConn);
            this.sparql = requireNonNull(sparql);
            this.numReads = Optional.empty();
        }

        /**
         * Constructs an instance of {@link QueryBenchmarkRun} that will only read a specific number of results.
         *
         * @param sailConn - The connection to the Rya instance the query will be executed against. (not null)
         * @param sparql - The query that will be executed. (not null)
         * @param numReads - The number of results that will be read. (not null)
         */
        public QueryBenchmarkRun(final SailConnection sailConn, final String sparql, final Long numReads) {
            this.sailConn = requireNonNull(sailConn);
            this.sparql = requireNonNull(sparql);
            this.numReads = Optional.of( requireNonNull(numReads) );
        }

        public void run() throws MalformedQueryException, QueryEvaluationException, NotEnoughResultsException, SailException {
            CloseableIteration<? extends BindingSet, QueryEvaluationException> it = null;

            try {
                // Execute the query.
                final SPARQLParser sparqlParser = new SPARQLParser();
                final ParsedQuery parsedQuery = sparqlParser.parseQuery(sparql, null);
                it = sailConn.evaluate(parsedQuery.getTupleExpr(), null, null, false);

                // Perform the reads.
                if(numReads.isPresent()) {
                    read(it, numReads.get() );
                } else {
                    readAll(it);
                }
            } finally {
                if(it != null) {
                    it.close();
                }
            }
        }

        private void read(final CloseableIteration<? extends BindingSet, QueryEvaluationException> it, final long numReads) throws QueryEvaluationException, NotEnoughResultsException {
            requireNonNull(it);
            long i = 0;
            while(i < numReads) {
                if(!it.hasNext()) {
                    throw new NotEnoughResultsException(String.format("The SPARQL query did not result in enough results. Needed: %d Found: %d", numReads, i));
                }
                it.next();
                i++;
            }
        }

        private  void readAll(final CloseableIteration<? extends BindingSet, QueryEvaluationException> it) throws QueryEvaluationException {
            requireNonNull(it);
            while(it.hasNext()) {
                it.next();
            }
        }

        /**
         * The benchmark must read a specific number of results, but the benchmarked query
         * does not have enough results to meet that number.
         */
        public static final class NotEnoughResultsException extends Exception {
            private static final long serialVersionUID = 1L;

            public NotEnoughResultsException(final String message) {
                super(message);
            }
        }
    }
}