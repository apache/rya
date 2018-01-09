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
package org.apache.rya.indexing.pcj.fluo.app.batch.serializer;

import static org.junit.Assert.assertEquals;

import java.util.Optional;

import org.apache.fluo.api.data.Bytes;
import org.apache.fluo.api.data.Span;
import org.apache.rya.api.function.join.LazyJoiningIterator.Side;
import org.apache.rya.api.model.VisibilityBindingSet;
import org.apache.rya.indexing.pcj.fluo.app.batch.BatchInformation;
import org.apache.rya.indexing.pcj.fluo.app.batch.BatchInformation.Task;
import org.apache.rya.indexing.pcj.fluo.app.batch.JoinBatchInformation;
import org.apache.rya.indexing.pcj.fluo.app.batch.SpanBatchDeleteInformation;
import org.apache.rya.indexing.pcj.fluo.app.query.FluoQueryColumns;
import org.apache.rya.indexing.pcj.fluo.app.query.JoinMetadata.JoinType;
import org.junit.Test;
import org.openrdf.model.impl.URIImpl;
import org.openrdf.query.algebra.evaluation.QueryBindingSet;

public class BatchInformationSerializerTest {

    @Test
    public void testSpanBatchInformationSerialization() {

        SpanBatchDeleteInformation batch = SpanBatchDeleteInformation.builder().setBatchSize(1000)
                .setColumn(FluoQueryColumns.PERIODIC_QUERY_BINDING_SET).setSpan(Span.prefix(Bytes.of("prefix"))).build();
        System.out.println(batch);
        byte[] batchBytes = BatchInformationSerializer.toBytes(batch);
        Optional<BatchInformation> decodedBatch = BatchInformationSerializer.fromBytes(batchBytes);
        System.out.println(decodedBatch);
        assertEquals(batch, decodedBatch.get());
    }

    @Test
    public void testJoinBatchInformationSerialization() {

        QueryBindingSet bs = new QueryBindingSet();
        bs.addBinding("a", new URIImpl("urn:123"));
        bs.addBinding("b", new URIImpl("urn:456"));
        VisibilityBindingSet vBis = new VisibilityBindingSet(bs, "FOUO");
        
        JoinBatchInformation batch = JoinBatchInformation.builder().setBatchSize(1000).setTask(Task.Update)
                .setColumn(FluoQueryColumns.PERIODIC_QUERY_BINDING_SET).setSpan(Span.prefix(Bytes.of("prefix346")))
                .setJoinType(JoinType.LEFT_OUTER_JOIN).setSide(Side.RIGHT).setBs(vBis).build();
        
        byte[] batchBytes = BatchInformationSerializer.toBytes(batch);
        Optional<BatchInformation> decodedBatch = BatchInformationSerializer.fromBytes(batchBytes);
        assertEquals(batch, decodedBatch.get());
    }

}
