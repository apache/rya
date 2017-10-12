package org.apache.rya.accumulo.pig;

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



import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.apache.accumulo.core.Constants;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.BatchWriterConfig;
import org.apache.accumulo.core.client.mapreduce.AccumuloInputFormat;
import org.apache.accumulo.core.client.mapreduce.AccumuloOutputFormat;
import org.apache.accumulo.core.client.mapreduce.lib.util.ConfiguratorBase;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.core.security.ColumnVisibility;
import org.apache.accumulo.core.util.Pair;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.pig.LoadFunc;
import org.apache.pig.OrderedLoadFunc;
import org.apache.pig.ResourceSchema;
import org.apache.pig.StoreFuncInterface;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.PigSplit;
import org.apache.pig.data.DataByteArray;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;

/**
 * A LoadStoreFunc for retrieving data from and storing data to Accumulo
 * <p/>
 * A Key/Val pair will be returned as tuples: (key, colfam, colqual, colvis, timestamp, value). All fields except timestamp are DataByteArray, timestamp is a long.
 * <p/>
 * Tuples can be written in 2 forms:
 * (key, colfam, colqual, colvis, value)
 * OR
 * (key, colfam, colqual, value)
 */
public class AccumuloStorage extends LoadFunc implements StoreFuncInterface, OrderedLoadFunc {
    private static final Log logger = LogFactory.getLog(AccumuloStorage.class);

    protected Configuration conf;
    protected RecordReader<Key, Value> reader;
    protected RecordWriter<Text, Mutation> writer;

    protected String inst;
    protected String zookeepers;
    protected String user = "";
    protected String password = "";
    protected String table;
    protected Text tableName;
    protected String auths;
    protected Authorizations authorizations = Constants.NO_AUTHS;
    protected List<Pair<Text, Text>> columnFamilyColumnQualifierPairs = new LinkedList<Pair<Text, Text>>();

    protected Collection<Range> ranges = new ArrayList<Range>();
    protected boolean mock = false;

    public AccumuloStorage() {
    }

    @Override
    public Tuple getNext() throws IOException {
        try {
            // load the next pair
            if (!reader.nextKeyValue()) {
                logger.info("Reached end of results");
                return null;
            }

            final Key key = reader.getCurrentKey();
            final Value value = reader.getCurrentValue();
            assert key != null && value != null;

            if (logger.isTraceEnabled()) {
                logger.trace("Found key[" + key + "] and value[" + value + "]");
            }

            // and wrap it in a tuple
            final Tuple tuple = TupleFactory.getInstance().newTuple(6);
            tuple.set(0, new DataByteArray(key.getRow().getBytes()));
            tuple.set(1, new DataByteArray(key.getColumnFamily().getBytes()));
            tuple.set(2, new DataByteArray(key.getColumnQualifier().getBytes()));
            tuple.set(3, new DataByteArray(key.getColumnVisibility().getBytes()));
            tuple.set(4, key.getTimestamp());
            tuple.set(5, new DataByteArray(value.get()));
            if (logger.isTraceEnabled()) {
                logger.trace("Output tuple[" + tuple + "]");
            }
            return tuple;
        } catch (final InterruptedException e) {
            throw new IOException(e.getMessage());
        }
    }

    @Override
    public InputFormat getInputFormat() {
        return new AccumuloInputFormat();
    }

    @Override
    public void prepareToRead(final RecordReader reader, final PigSplit split) {
        this.reader = reader;
    }

    @Override
    public void setLocation(final String location, final Job job) throws IOException {
        if (logger.isDebugEnabled()) {
            logger.debug("Set Location[" + location + "] for job[" + job.getJobName() + "]");
        }
        conf = job.getConfiguration();
        setLocationFromUri(location, job);

        if (!ConfiguratorBase.isConnectorInfoSet(AccumuloInputFormat.class, conf)) {
            try {
				AccumuloInputFormat.setConnectorInfo(job, user, new PasswordToken(password.getBytes(StandardCharsets.UTF_8)));
			} catch (final AccumuloSecurityException e) {
				throw new RuntimeException(e);
			}
            AccumuloInputFormat.setInputTableName(job, table);
    		AccumuloInputFormat.setScanAuthorizations(job, authorizations);
            if (!mock) {
                AccumuloInputFormat.setZooKeeperInstance(job, inst, zookeepers);
            } else {
                AccumuloInputFormat.setMockInstance(job, inst);
            }
        }
        if (columnFamilyColumnQualifierPairs.size() > 0) {
            AccumuloInputFormat.fetchColumns(job, columnFamilyColumnQualifierPairs);
        }
        logger.info("Set ranges[" + ranges + "] for job[" + job.getJobName() + "] on table[" + table + "] " +
                "for columns[" + columnFamilyColumnQualifierPairs + "] with authorizations[" + authorizations + "]");

        if (ranges.size() == 0) {
            throw new IOException("Accumulo Range must be specified");
        }
        AccumuloInputFormat.setRanges(job, ranges);
    }

    protected void setLocationFromUri(final String uri, final Job job) throws IOException {
        // ex: accumulo://table1?instance=myinstance&user=root&password=secret&zookeepers=127.0.0.1:2181&auths=PRIVATE,PUBLIC&columns=col1|cq1,col2|cq2&range=a|z&range=1|9&mock=true
        try {
            if (!uri.startsWith("accumulo://")) {
                throw new Exception("Bad scheme.");
            }
            final String[] urlParts = uri.split("\\?");
            setLocationFromUriParts(urlParts);

        } catch (final Exception e) {
            throw new IOException("Expected 'accumulo://<table>[?instance=<instanceName>&user=<user>&password=<password>&zookeepers=<zookeepers>&auths=<authorizations>&[range=startRow|endRow[...],columns=[cf1|cq1,cf2|cq2,...]],mock=true(false)]': " + e.getMessage(), e);
        }
    }

    protected void setLocationFromUriParts(final String[] urlParts) {
        String columns = "";
        if (urlParts.length > 1) {
            for (final String param : urlParts[1].split("&")) {
                final String[] pair = param.split("=");
                if (pair[0].equals("instance")) {
                    inst = pair[1];
                } else if (pair[0].equals("user")) {
                    user = pair[1];
                } else if (pair[0].equals("password")) {
                    password = pair[1];
                } else if (pair[0].equals("zookeepers")) {
                    zookeepers = pair[1];
                } else if (pair[0].equals("auths")) {
                    auths = pair[1];
                } else if (pair[0].equals("columns")) {
                    columns = pair[1];
                } else if (pair[0].equals("range")) {
                    final String[] r = pair[1].split("\\|");
                    if (r.length == 2) {
                        addRange(new Range(r[0], r[1]));
                    } else {
                        addRange(new Range(r[0]));
                    }
                } else if (pair[0].equals("mock")) {
                    this.mock = Boolean.parseBoolean(pair[1]);
                }
                addLocationFromUriPart(pair);
            }
        }
        final String[] parts = urlParts[0].split("/+");
        table = parts[1];
        tableName = new Text(table);

        if (auths == null || auths.equals("")) {
            authorizations = new Authorizations();
        } else {
            authorizations = new Authorizations(auths.split(","));
        }

        if (!columns.equals("")) {
            for (final String cfCq : columns.split(",")) {
                if (cfCq.contains("|")) {
                    final String[] c = cfCq.split("\\|");
                    final String cf = c[0];
                    final String cq = c[1];
                    addColumnPair(cf, cq);
                } else {
                    addColumnPair(cfCq, null);
                }
            }
        }
    }

    protected void addColumnPair(final String cf, final String cq) {
        columnFamilyColumnQualifierPairs.add(new Pair<Text, Text>((cf != null) ? new Text(cf) : null, (cq != null) ? new Text(cq) : null));
    }

    protected void addLocationFromUriPart(final String[] pair) {

    }

    protected void addRange(final Range range) {
        ranges.add(range);
    }

    @Override
    public String relativeToAbsolutePath(final String location, final Path curDir) throws IOException {
        return location;
    }

    @Override
    public void setUDFContextSignature(final String signature) {

    }

    /* StoreFunc methods */
    @Override
    public void setStoreFuncUDFContextSignature(final String signature) {

    }

    @Override
    public String relToAbsPathForStoreLocation(final String location, final Path curDir) throws IOException {
        return relativeToAbsolutePath(location, curDir);
    }

    @Override
    public void setStoreLocation(final String location, final Job job) throws IOException {
        conf = job.getConfiguration();
        setLocationFromUri(location, job);

        if (!conf.getBoolean(AccumuloOutputFormat.class.getSimpleName() + ".configured", false)) {
            try {
				AccumuloOutputFormat.setConnectorInfo(job, user, new PasswordToken(password.getBytes(StandardCharsets.UTF_8)));
			} catch (final AccumuloSecurityException e) {
				throw new RuntimeException(e);
			}
            AccumuloOutputFormat.setDefaultTableName(job, table);
            AccumuloOutputFormat.setZooKeeperInstance(job, inst, zookeepers);
            final BatchWriterConfig config = new BatchWriterConfig();
            config.setMaxLatency(10, TimeUnit.SECONDS);
            config.setMaxMemory(10 * 1000 * 1000);
            config.setMaxWriteThreads(10);
            AccumuloOutputFormat.setBatchWriterOptions(job, config);
        }
    }

    @Override
    public OutputFormat getOutputFormat() {
        return new AccumuloOutputFormat();
    }

    @Override
    public void checkSchema(final ResourceSchema schema) throws IOException {
        // we don't care about types, they all get casted to ByteBuffers
    }

    @Override
    public void prepareToWrite(final RecordWriter writer) {
        this.writer = writer;
    }

    @Override
    public void putNext(final Tuple t) throws ExecException, IOException {
        final Mutation mut = new Mutation(objToText(t.get(0)));
        final Text cf = objToText(t.get(1));
        final Text cq = objToText(t.get(2));

        if (t.size() > 4) {
            final Text cv = objToText(t.get(3));
            final Value val = new Value(objToBytes(t.get(4)));
            if (cv.getLength() == 0) {
                mut.put(cf, cq, val);
            } else {
                mut.put(cf, cq, new ColumnVisibility(cv), val);
            }
        } else {
            final Value val = new Value(objToBytes(t.get(3)));
            mut.put(cf, cq, val);
        }

        try {
            writer.write(tableName, mut);
        } catch (final InterruptedException e) {
            throw new IOException(e);
        }
    }

    private static Text objToText(final Object o) {
        return new Text(objToBytes(o));
    }

    private static byte[] objToBytes(final Object o) {
        if (o instanceof String) {
            final String str = (String) o;
            return str.getBytes(StandardCharsets.UTF_8);
        } else if (o instanceof Long) {
            final Long l = (Long) o;
            return l.toString().getBytes(StandardCharsets.UTF_8);
        } else if (o instanceof Integer) {
            final Integer l = (Integer) o;
            return l.toString().getBytes(StandardCharsets.UTF_8);
        } else if (o instanceof Boolean) {
            final Boolean l = (Boolean) o;
            return l.toString().getBytes(StandardCharsets.UTF_8);
        } else if (o instanceof Float) {
            final Float l = (Float) o;
            return l.toString().getBytes(StandardCharsets.UTF_8);
        } else if (o instanceof Double) {
            final Double l = (Double) o;
            return l.toString().getBytes(StandardCharsets.UTF_8);
        }

        // TODO: handle DataBag, Map<Object, Object>, and Tuple

        return ((DataByteArray) o).get();
    }

    @Override
    public void cleanupOnFailure(final String failure, final Job job) {
    }

    @Override
    public WritableComparable<?> getSplitComparable(final InputSplit inputSplit) throws IOException {
        //cannot get access to the range directly
        final AccumuloInputFormat.RangeInputSplit rangeInputSplit = (AccumuloInputFormat.RangeInputSplit) inputSplit;
        final ByteArrayOutputStream baos = new ByteArrayOutputStream();
        final DataOutputStream out = new DataOutputStream(baos);
        rangeInputSplit.write(out);
        out.close();
        final DataInputStream stream = new DataInputStream(new ByteArrayInputStream(baos.toByteArray()));
        final Range range = new Range();
        range.readFields(stream);
        stream.close();
        return range;
    }
}
