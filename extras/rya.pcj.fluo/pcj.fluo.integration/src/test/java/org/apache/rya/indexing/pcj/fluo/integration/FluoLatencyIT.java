package org.apache.rya.indexing.pcj.fluo.integration;

import static java.util.Objects.requireNonNull;

import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.xml.datatype.DatatypeConfigurationException;
import javax.xml.datatype.DatatypeFactory;

import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.fluo.api.client.FluoClient;
import org.apache.fluo.api.client.FluoFactory;
import org.apache.fluo.api.client.Transaction;
import org.apache.fluo.api.client.scanner.CellScanner;
import org.apache.fluo.api.config.FluoConfiguration;
import org.apache.fluo.api.data.RowColumnValue;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.rya.api.client.CreatePCJ.ExportStrategy;
import org.apache.rya.indexing.pcj.fluo.api.CreateFluoPcj;
import org.apache.rya.indexing.pcj.fluo.app.util.FluoQueryUtils;
import org.apache.rya.indexing.pcj.storage.accumulo.VisibilityBindingSet;
import org.apache.rya.pcj.fluo.test.base.KafkaExportITBase;
import org.junit.BeforeClass;
import org.junit.Test;
import org.openrdf.model.Literal;
import org.openrdf.model.Statement;
import org.openrdf.model.ValueFactory;
import org.openrdf.model.impl.ValueFactoryImpl;
import org.openrdf.repository.sail.SailRepositoryConnection;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

public class FluoLatencyIT extends KafkaExportITBase {
    private static ValueFactory vf;
    private static DatatypeFactory dtf;

    @BeforeClass
    public static void init() throws DatatypeConfigurationException {
        vf = new ValueFactoryImpl();
        dtf = DatatypeFactory.newInstance();
    }

    @Test
    public void resultsExported() throws Exception {

        final String sparql = "prefix time: <http://www.w3.org/2006/time#> " + "select ?type (count(?obs) as ?total) where { "
                + "    ?obs <uri:hasTime> ?time. " + "    ?obs <uri:hasObsType> ?type " + "} " + "group by ?type";

//        final String sparql = "prefix time: <http://www.w3.org/2006/time#> " + "select ?type ?obs where { "
//                + "    ?obs <uri:hasTime> ?time. " + "    ?obs <uri:hasObsType> ?type " + "}";

        try (FluoClient fluoClient = FluoFactory.newClient(super.getFluoConfiguration())) {
            // Tell the Fluo app to maintain the PCJ.
            String pcjId = FluoQueryUtils.createNewPcjId();
            FluoConfiguration conf = super.getFluoConfiguration();
            new CreateFluoPcj().createPcj(pcjId, sparql, Sets.newHashSet(ExportStrategy.KAFKA), fluoClient);
            SailRepositoryConnection conn = super.getRyaSailRepository().getConnection();

            long start = System.currentTimeMillis();
            int numReturned = 0;
            int numObs = 10;
            int numTypes = 5;
            int numExpected = 0;
            int increment = numObs*numTypes;
            while (System.currentTimeMillis() - start < 60000) {
                List<Statement> statements = generate(10, 5, "car_", numExpected, ZonedDateTime.now());
                conn.add(statements);
                numExpected += increment;
                System.out.println("Num Accumulo Entries: " + getNumAccEntries(conf.getAccumuloTable()) + " Num Fluo Entries: "
                        + getNumFluoEntries(fluoClient));
                numReturned += readAllResults(pcjId).size();
                System.out
                        .println("Expected: " + numExpected + " NumReturned: " + numReturned + " Difference: " + (numExpected - numReturned));
//                FluoITHelper.printFluoTable(conf);
                Thread.sleep(30000);
            }
        }
    }

    /**
     * Generates (numObservationsPerType x numTypes) statements of the form:
     *
     * <pre>
     * urn:obs_n uri:hasTime zonedTime
     * urn:obs_n uri:hasObsType typePrefix_m
     * </pre>
     *
     * Where the n in urn:obs_n is the ith value in 0 to (numObservationsPerType x numTypes) with an offset specified by
     * observationOffset, and where m in typePrefix_m is the jth value in 0 to numTypes.
     *
     * @param numObservationsPerType - The quantity of observations per type to generate.
     * @param numTypes - The number of types to generate observations for.
     * @param typePrefix - The prefix to be used for the type literal in the statement.
     * @param observationOffset - The offset to be used for determining the value of n in the above statements.
     * @param zonedTime - The time to be used for all observations generated.
     * @return A new list of all generated Statements.
     */
    public List<Statement> generate(final long numObservationsPerType, final int numTypes, final String typePrefix,
            final long observationOffset, final ZonedDateTime zonedTime) {
        final String time = zonedTime.format(DateTimeFormatter.ISO_INSTANT);
        final Literal litTime = vf.createLiteral(dtf.newXMLGregorianCalendar(time));
        final List<Statement> statements = Lists.newArrayList();

        for (long i = 0; i < numObservationsPerType; i++) {
            for (int j = 0; j < numTypes; j++) {
                final long observationId = observationOffset + i * numTypes + j;
                // final String obsId = "urn:obs_" + Long.toHexString(observationId) + "_" + observationId;
                // final String obsId = "urn:obs_" + observationId;
                final String obsId = "urn:obs_" + String.format("%020d", observationId);
                final String type = typePrefix + j;
                // logger.info(obsId + " " + type + " " + litTime);
                statements.add(vf.createStatement(vf.createURI(obsId), vf.createURI("uri:hasTime"), litTime));
                statements.add(vf.createStatement(vf.createURI(obsId), vf.createURI("uri:hasObsType"), vf.createLiteral(type)));
            }
        }

        return statements;
    }

    private Set<VisibilityBindingSet> readAllResults(final String pcjId) throws Exception {
        requireNonNull(pcjId);

        // Read all of the results from the Kafka topic.
        final Set<VisibilityBindingSet> results = new HashSet<>();

        try (final KafkaConsumer<String, VisibilityBindingSet> consumer = makeConsumer(pcjId)) {
            final ConsumerRecords<String, VisibilityBindingSet> records = consumer.poll(5000);
            final Iterator<ConsumerRecord<String, VisibilityBindingSet>> recordIterator = records.iterator();
            while (recordIterator.hasNext()) {
                results.add(recordIterator.next().value());
            }
        }

        return results;
    }

    private int getNumAccEntries(String tableName) throws TableNotFoundException {
        Scanner scanner = super.getAccumuloConnector().createScanner(tableName, new Authorizations());
        int count = 0;
        for (Map.Entry<Key, Value> entry : scanner) {
            count++;
        }
        return count;
    }

    private int getNumFluoEntries(FluoClient client) {
        Transaction tx = client.newTransaction();
        CellScanner scanner = tx.scanner().build();
        int count = 0;
        for (RowColumnValue rcv : scanner) {
            count++;
        }
        return count;
    }

}
