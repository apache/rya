package org.apache.rya.accumulo.mr.tools;

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



import org.apache.rya.accumulo.mr.AbstractAccumuloMRTool;
import org.apache.rya.accumulo.mr.MRUtils;

import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.client.mapreduce.AccumuloInputFormat;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.user.RegExFilter;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.calrissian.mango.types.LexiTypeEncoders;
import org.calrissian.mango.types.TypeEncoder;

import java.io.IOException;
import java.util.Date;

import static org.apache.rya.api.RdfCloudTripleStoreConstants.*;

/**
 */
public class Upgrade322Tool extends AbstractAccumuloMRTool implements Tool {
    @Override
    public int run(String[] strings) throws Exception {
        conf.set(MRUtils.JOB_NAME_PROP, "Upgrade to Rya 3.2.2");
        //faster
        init();

        Job job = new Job(conf);
        job.setJarByClass(Upgrade322Tool.class);

        setupAccumuloInput(job);
        AccumuloInputFormat.setInputTableName(job, MRUtils.getTablePrefix(conf) + TBL_OSP_SUFFIX);

        //we do not need to change any row that is a string, custom, or uri type
        IteratorSetting regex = new IteratorSetting(30, "regex",
                                                    RegExFilter.class);
        RegExFilter.setRegexs(regex, "\\w*" + TYPE_DELIM + "[\u0003|\u0008|\u0002]", null, null, null, false);
        RegExFilter.setNegate(regex, true);

        // set input output of the particular job
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Mutation.class);

        setupAccumuloOutput(job, MRUtils.getTablePrefix(conf) +
                               TBL_SPO_SUFFIX);

        // set mapper and reducer classes
        job.setMapperClass(Upgrade322Mapper.class);
        job.setReducerClass(Reducer.class);

        // Submit the job
        return job.waitForCompletion(true) ? 0 : 1;
    }

    public static void main(String[] args) {
        try {
            ToolRunner.run(new Configuration(), new Upgrade322Tool(), args);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /**
     * Reading from the OSP table
     */
    public static class Upgrade322Mapper extends Mapper<Key, Value, Text, Mutation> {

        private String tablePrefix;
        private Text spoTable;
        private Text poTable;
        private Text ospTable;

        private final UpgradeObjectSerialization upgradeObjectSerialization;

        public Upgrade322Mapper() {
            this(new UpgradeObjectSerialization());
        }

        public Upgrade322Mapper(
          UpgradeObjectSerialization upgradeObjectSerialization) {
            this.upgradeObjectSerialization = upgradeObjectSerialization;
        }

        @Override
        protected void setup(
          Context context) throws IOException, InterruptedException {
            super.setup(context);

            tablePrefix = context.getConfiguration().get(
              MRUtils.TABLE_PREFIX_PROPERTY, TBL_PRFX_DEF);
            spoTable = new Text(tablePrefix + TBL_SPO_SUFFIX);
            poTable = new Text(tablePrefix + TBL_PO_SUFFIX);
            ospTable = new Text(tablePrefix + TBL_OSP_SUFFIX);
        }

        @Override
        protected void map(
          Key key, Value value, Context context)
          throws IOException, InterruptedException {

            //read the key, expect OSP
            final String row = key.getRow().toString();
            final int firstDelim = row.indexOf(DELIM);
            final int secondDelim = row.indexOf(DELIM, firstDelim + 1);
            final int typeDelim = row.lastIndexOf(TYPE_DELIM);
            final String oldSerialization = row.substring(0, firstDelim);
            char typeMarker = row.charAt(row.length() - 1);

            final String subject = row.substring(firstDelim + 1, secondDelim);
            final String predicate = row.substring(secondDelim + 1, typeDelim);
            final String typeSuffix = TYPE_DELIM + typeMarker;

            String newSerialization = upgradeObjectSerialization.upgrade(oldSerialization, typeMarker);
            if(newSerialization == null) {
                return;
            }

            //write out delete Mutations
            Mutation deleteOldSerialization_osp = new Mutation(key.getRow());
            deleteOldSerialization_osp.putDelete(key.getColumnFamily(), key.getColumnQualifier(),
                               key.getColumnVisibilityParsed());
            Mutation deleteOldSerialization_po = new Mutation(predicate + DELIM + oldSerialization + DELIM + subject + typeSuffix);
            deleteOldSerialization_po.putDelete(key.getColumnFamily(),
                                                key.getColumnQualifier(),
                                                key.getColumnVisibilityParsed());
            Mutation deleteOldSerialization_spo = new Mutation(subject + DELIM + predicate + DELIM + oldSerialization + typeSuffix);
            deleteOldSerialization_spo.putDelete(key.getColumnFamily(), key.getColumnQualifier(),
                                                key.getColumnVisibilityParsed());

            //write out new serialization
            Mutation putNewSerialization_osp = new Mutation(newSerialization + DELIM + subject + DELIM + predicate + typeSuffix);
            putNewSerialization_osp.put(key.getColumnFamily(),
                                        key.getColumnQualifier(),
                                        key.getColumnVisibilityParsed(),
                                        key.getTimestamp(), value);
            Mutation putNewSerialization_po = new Mutation(predicate + DELIM + newSerialization + DELIM + subject + typeSuffix);
            putNewSerialization_po.put(key.getColumnFamily(),
                                       key.getColumnQualifier(),
                                       key.getColumnVisibilityParsed(),
                                       key.getTimestamp(), value);
            Mutation putNewSerialization_spo = new Mutation(subject + DELIM + predicate + DELIM + newSerialization + typeSuffix);
            putNewSerialization_spo.put(key.getColumnFamily(),
                                        key.getColumnQualifier(),
                                        key.getColumnVisibilityParsed(),
                                        key.getTimestamp(), value);

            //write out deletes to all tables
            context.write(ospTable, deleteOldSerialization_osp);
            context.write(poTable, deleteOldSerialization_po);
            context.write(spoTable, deleteOldSerialization_spo);

            //write out inserts to all tables
            context.write(ospTable, putNewSerialization_osp);
            context.write(poTable, putNewSerialization_po);
            context.write(spoTable, putNewSerialization_spo);
        }
    }

    public static class UpgradeObjectSerialization {

        public static final TypeEncoder<Boolean, String>
          BOOLEAN_STRING_TYPE_ENCODER = LexiTypeEncoders.booleanEncoder();
        public static final TypeEncoder<Byte, String> BYTE_STRING_TYPE_ENCODER
          = LexiTypeEncoders.byteEncoder();
        public static final TypeEncoder<Date, String> DATE_STRING_TYPE_ENCODER
          = LexiTypeEncoders.dateEncoder();
        public static final TypeEncoder<Integer, String>
          INTEGER_STRING_TYPE_ENCODER = LexiTypeEncoders.integerEncoder();
        public static final TypeEncoder<Long, String> LONG_STRING_TYPE_ENCODER
          = LexiTypeEncoders.longEncoder();
        public static final TypeEncoder<Double, String>
          DOUBLE_STRING_TYPE_ENCODER = LexiTypeEncoders.doubleEncoder();

        public String upgrade(String object, int typeMarker) {
            switch(typeMarker) {
                case 10: //boolean
                    final boolean bool = Boolean.parseBoolean(object);
                    return BOOLEAN_STRING_TYPE_ENCODER.encode(bool);
                case 9: //byte
                    final byte b = Byte.parseByte(object);
                    return BYTE_STRING_TYPE_ENCODER.encode(b);
                case 4: //long
                    final Long lng = Long.parseLong(object);
                    return LONG_STRING_TYPE_ENCODER.encode(lng);
                case 5: //int
                    final Integer i = Integer.parseInt(object);
                    return INTEGER_STRING_TYPE_ENCODER.encode(i);
                case 6: //double
                    String exp = object.substring(2, 5);
                    char valueSign = object.charAt(0);
                    char expSign = object.charAt(1);
                    Integer expInt = Integer.parseInt(exp);
                    if (expSign == '-') {
                        expInt = 999 - expInt;
                    }
                    final String expDoubleStr =
                      String.format("%s%sE%s%d", valueSign,
                                    object.substring(6),
                                    expSign, expInt);
                    return DOUBLE_STRING_TYPE_ENCODER
                      .encode(Double.parseDouble(expDoubleStr));
                case 7: //datetime
                    //check to see if it is an early release that includes the exact term xsd:dateTime
                    final Long l = Long.MAX_VALUE - Long.parseLong(object);
                    Date date = new Date(l);
                    return DATE_STRING_TYPE_ENCODER.encode(date);
                default:
                    return null;
            }
        }
    }
}
