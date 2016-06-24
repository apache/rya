//package mvm.rya.accumulo.mr;
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

//
//import java.io.IOException;
//import java.util.List;
//import java.util.Map.Entry;
//import java.util.SortedMap;
//
//import org.apache.accumulo.core.client.Instance;
//import org.apache.accumulo.core.client.IteratorSetting;
//import org.apache.accumulo.core.client.Scanner;
//import org.apache.accumulo.core.client.mapred.AccumuloInputFormat;
//import org.apache.accumulo.core.client.mapreduce.AbstractInputFormat;
//import org.apache.accumulo.core.client.mapreduce.RangeInputSplit;
//import org.apache.accumulo.core.client.mapreduce.lib.util.InputConfigurator;
//import org.apache.accumulo.core.data.Key;
//import org.apache.accumulo.core.data.Value;
//import org.apache.accumulo.core.iterators.user.WholeRowIterator;
//import org.apache.hadoop.io.Text;
//import org.apache.hadoop.mapreduce.InputSplit;
//import org.apache.hadoop.mapreduce.JobContext;
//import org.apache.hadoop.mapreduce.RecordReader;
//import org.apache.hadoop.mapreduce.TaskAttemptContext;
//
//import mvm.rya.accumulo.AccumuloRdfConfiguration;
//import mvm.rya.api.domain.RyaStatement;
//import mvm.rya.api.resolver.RyaTripleContext;
//import mvm.rya.api.resolver.triple.TripleRow;
//import mvm.rya.api.resolver.triple.TripleRowResolverException;
//import mvm.rya.indexing.accumulo.ConfigUtils;
//
//public class EntityInputFormat extends AbstractInputFormat<Text, RyaEntity> {
//    @Override
//    public RecordReader<Text, RyaEntity> createRecordReader(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException {
//        return new RyaStatementRecordReader();
//    }
//
//    public List<InputSplit> getSplits(JobContext context) throws IOException {
//
//        Instance instance = getInstance(context);
//        String entityCentricTableName = ConfigUtils.getEntityTableName(context.getConfiguration());
//        InputConfigurator.setInputTableName(AccumuloInputFormat.class, context.getConfiguration(), entityCentricTableName);
//
//        // Get the splits for the range
//        return super.getSplits(context);
//        
//    }
//
//    public class RyaStatementRecordReader extends AbstractRecordReader<Text, RyaEntity> {
//
//        @Override
//        protected void setupIterators(TaskAttemptContext context, Scanner scanner, String tableName, RangeInputSplit split) {
//        	// copy existing iterators
//            List<IteratorSetting> iterators = split.getIterators();
//            for (IteratorSetting setting : iterators) {
//                scanner.addScanIterator(setting);
//            }
//            // add the whole row iterator
//            scanner.addScanIterator(new IteratorSetting(21, "wholerow", WholeRowIterator.class));	
//        }
//
//        @Override
//        public void initialize(InputSplit inSplit, TaskAttemptContext attempt) throws IOException {
//            super.initialize(inSplit, attempt);
//        }
//
//        @Override
//        public boolean nextKeyValue() throws IOException {
//            if (!scannerIterator.hasNext())
//                return false;
//
//            Entry<Key, Value> entry = scannerIterator.next();
//            ++numKeysRead;
//            currentKey = entry.getKey();
//
//            try {
//                SortedMap<Key, Value> wholeRow = WholeRowIterator.decodeRow(entry.getKey(), entry.getValue());
//                for (Entry<Key,Value> rowEntry : wholeRow.entrySet()){
//                	// figure out the subj/obj for this row
////                	rowEntry
//                }
////                RyaStatement stmt = 
//                
//                
//                currentV = new RyaEntity();
//            } catch(TripleRowResolveExceptiorException e) {
//                throw new IOException(e);
//            }
//            return true;
//        }
//
//    }
//
//}
