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

package org.apache.rya.prospector.plans.impl

import org.apache.rya.api.domain.RyaStatement
import org.apache.rya.prospector.domain.IndexEntry
import org.apache.rya.prospector.domain.IntermediateProspect
import org.apache.rya.prospector.domain.TripleValueType
import org.apache.rya.prospector.plans.IndexWorkPlan
import org.apache.rya.prospector.utils.CustomEntry
import org.apache.rya.prospector.utils.ProspectorUtils

import org.apache.accumulo.core.data.Mutation
import org.apache.accumulo.core.data.Range
import org.apache.accumulo.core.data.Value
import org.apache.accumulo.core.security.Authorizations
import org.apache.accumulo.core.security.ColumnVisibility
import org.apache.hadoop.io.LongWritable
import org.apache.hadoop.io.Text
import org.apache.hadoop.mapreduce.Reducer
import org.openrdf.model.util.URIUtil
import org.openrdf.model.vocabulary.XMLSchema;

import static org.apache.rya.prospector.utils.ProspectorConstants.COUNT;
import org.apache.rya.api.RdfCloudTripleStoreConstants

/**
 * Date: 12/3/12
 * Time: 12:28 PM
 */
class CountPlan implements IndexWorkPlan {

    @Override
    Collection<Map.Entry<IntermediateProspect, LongWritable>> map(RyaStatement ryaStatement) {
        def subject = ryaStatement.getSubject()
        def predicate = ryaStatement.getPredicate()
		def subjpred = ryaStatement.getSubject().data + DELIM + ryaStatement.getPredicate().data
		def predobj = ryaStatement.getPredicate().data + DELIM + ryaStatement.getObject().data
		def subjobj = ryaStatement.getSubject().data + DELIM + ryaStatement.getObject().data
        def object = ryaStatement.getObject()
        def localIndex = URIUtil.getLocalNameIndex(subject.data)
        def namespace = subject.data.substring(0, localIndex - 1)
        def visibility = new String(ryaStatement.columnVisibility)
        return [
                new CustomEntry<IntermediateProspect, LongWritable>(
                        new IntermediateProspect(index: COUNT,
                                data: subject.data,
                                dataType: URITYPE,
                                tripleValueType: TripleValueType.subject,
                                visibility: visibility),
                        ONE),
                new CustomEntry<IntermediateProspect, LongWritable>(
                        new IntermediateProspect(index: COUNT,
                                data: predicate.data,
                                dataType: URITYPE,
                                tripleValueType: TripleValueType.predicate,
                                visibility: visibility
                        ), ONE),
                new CustomEntry<IntermediateProspect, LongWritable>(
                        new IntermediateProspect(index: COUNT,
                                data: object.data,
                                dataType: object.dataType.stringValue(),
                                tripleValueType: TripleValueType.object,
                                visibility: visibility
                        ), ONE),
                new CustomEntry<IntermediateProspect, LongWritable>(
                        new IntermediateProspect(index: COUNT,
                                data: subjpred,
                                dataType: XMLSchema.STRING,
                                tripleValueType: TripleValueType.subjectpredicate,
                                visibility: visibility
                        ), ONE),
                new CustomEntry<IntermediateProspect, LongWritable>(
                        new IntermediateProspect(index: COUNT,
                                data: subjobj,
                                dataType: XMLSchema.STRING,
                                tripleValueType: TripleValueType.subjectobject,
                                visibility: visibility
                        ), ONE),
                new CustomEntry<IntermediateProspect, LongWritable>(
                        new IntermediateProspect(index: COUNT,
                                data: predobj,
                                dataType: XMLSchema.STRING,
                                tripleValueType: TripleValueType.predicateobject,
                                visibility: visibility
                        ), ONE),
                new CustomEntry<IntermediateProspect, LongWritable>(
                        new IntermediateProspect(index: COUNT,
                                data: namespace,
                                dataType: URITYPE,
                                tripleValueType: TripleValueType.entity,
                                visibility: visibility
                        ), ONE),
        ]
    }

    @Override
    Collection<Map.Entry<IntermediateProspect, LongWritable>> combine(IntermediateProspect prospect, Iterable<LongWritable> counts) {

        def iter = counts.iterator()
        long sum = 0;
        iter.each { lw ->
            sum += lw.get()
        }

        return [new CustomEntry<IntermediateProspect, LongWritable>(prospect, new LongWritable(sum))]
    }

    @Override
    void reduce(IntermediateProspect prospect, Iterable<LongWritable> counts, Date timestamp, Reducer.Context context) {
        def iter = counts.iterator()
        long sum = 0;
        iter.each { lw ->
            sum += lw.get()
        }

        def indexType = prospect.tripleValueType.name()

		// not sure if this is the best idea..
        if ((sum >= 0) ||
                indexType.equals(TripleValueType.predicate.toString())) {

            Mutation m = new Mutation(indexType + DELIM + prospect.data + DELIM + ProspectorUtils.getReverseIndexDateTime(timestamp))
            m.put(COUNT, prospect.dataType, new ColumnVisibility(prospect.visibility), timestamp.getTime(), new Value("${sum}".getBytes()));

            context.write(null, m);
        }
    }

    @Override
    String getIndexType() {
        return COUNT
    }
	
    @Override
	String getCompositeValue(List<String> indices){
		Iterator<String> indexIt = indices.iterator();
		String compositeIndex = indexIt.next();
		while (indexIt.hasNext()){
			String value = indexIt.next();
			compositeIndex += DELIM + value;
		}
		return compositeIndex;
	}

    @Override
    List<IndexEntry> query(def connector, String tableName, List<Long> prospectTimes, String type, String compositeIndex, String dataType, String[] auths) {

        assert connector != null && tableName != null && type != null && compositeIndex != null
		
        def bs = connector.createBatchScanner(tableName, new Authorizations(auths), 4)
        def ranges = []
        int max = 1000; //by default only return 1000 prospects maximum
        if (prospectTimes != null) {
            prospectTimes.each { prospect ->
                ranges.add(
                        new Range(type + DELIM + compositeIndex + DELIM + ProspectorUtils.getReverseIndexDateTime(new Date(prospect))))
            }
        } else {
            max = 1; //only return the latest if no prospectTimes given
            def prefix = type + DELIM + compositeIndex + DELIM;
            ranges.add(new Range(prefix, prefix + RdfCloudTripleStoreConstants.LAST))
        }
        bs.ranges = ranges
        if (dataType != null) {
            bs.fetchColumn(new Text(COUNT), new Text(dataType))
        } else {
            bs.fetchColumnFamily(new Text(COUNT))
        }
		
        List<IndexEntry> indexEntries = new ArrayList<IndexEntry>()
        def iter = bs.iterator()
		
        while (iter.hasNext() && indexEntries.size() <= max) {
            def entry = iter.next()
            def k = entry.key
            def v = entry.value

            def rowArr = k.row.toString().split(DELIM)
			String values = "";
			// if it is a composite index, then return the type as a composite index
			if (type.equalsIgnoreCase(TripleValueType.subjectpredicate.toString()) || 
				type.equalsIgnoreCase(TripleValueType.subjectobject.toString()) ||
				type.equalsIgnoreCase(TripleValueType.predicateobject.toString())){
				values =rowArr[1] + DELIM + rowArr[2]
			}
			else values = rowArr[1]

            indexEntries.add(new IndexEntry(data: values,
                    tripleValueType: rowArr[0],
                    index: COUNT,
                    dataType: k.columnQualifier.toString(),
                    visibility: k.columnVisibility.toString(),
                    count: Long.parseLong(new String(v.get())),
                    timestamp: k.timestamp
            ))
        }
        bs.close()

        return indexEntries
    }

}
