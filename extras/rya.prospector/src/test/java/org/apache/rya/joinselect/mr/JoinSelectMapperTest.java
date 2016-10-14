package org.apache.rya.joinselect.mr;

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



import java.io.IOException;
import java.util.Map;

import org.apache.rya.joinselect.mr.JoinSelectSpoTableOutput;
import org.apache.rya.joinselect.mr.utils.CompositeType;
import org.apache.rya.joinselect.mr.utils.TripleCard;
import org.apache.rya.joinselect.mr.utils.TripleEntry;
import org.apache.rya.api.RdfCloudTripleStoreConstants.TABLE_LAYOUT;
import org.apache.rya.api.domain.RyaStatement;
import org.apache.rya.api.domain.RyaType;
import org.apache.rya.api.domain.RyaURI;
import org.apache.rya.api.resolver.triple.TripleRow;
import org.apache.rya.api.resolver.triple.TripleRowResolver;
import org.apache.rya.api.resolver.triple.TripleRowResolverException;
import org.apache.rya.api.resolver.triple.impl.WholeRowTripleResolver;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.junit.Test;

public class JoinSelectMapperTest {

  private static final String DELIM = "\u0000";

  @Test
  public void testOutput() throws TripleRowResolverException, IOException {

    RyaStatement rya = new RyaStatement(new RyaURI("urn:gem:etype#1234"), new RyaURI("urn:gem#pred"), new RyaType("mydata1"));
    Text s = new Text(rya.getSubject().getData());
    Text p = new Text(rya.getPredicate().getData());
    Text o = new Text(rya.getObject().getData());
    Text sp = new Text(rya.getSubject().getData() + DELIM + rya.getPredicate().getData());
    Text so = new Text(rya.getSubject().getData() + DELIM + rya.getObject().getData());
    Text po = new Text(rya.getPredicate().getData() + DELIM + rya.getObject().getData());
    Text ps = new Text(rya.getPredicate().getData() + DELIM + rya.getSubject().getData());
    Text op = new Text(rya.getObject().getData() + DELIM + rya.getPredicate().getData());
    Text os = new Text(rya.getObject().getData() + DELIM + rya.getSubject().getData());

    TripleEntry t1 = new TripleEntry(s, p, new Text("subject"), new Text("predicate"), new Text("object"));
    TripleEntry t2 = new TripleEntry(p, o, new Text("predicate"), new Text("object"), new Text("subject"));
    TripleEntry t3 = new TripleEntry(o, s, new Text("object"), new Text("subject"), new Text("predicate"));
    TripleEntry t4 = new TripleEntry(o, new Text(""), new Text("object"), new Text(""), new Text("subjectpredicate"));
    TripleEntry t5 = new TripleEntry(p, new Text(""), new Text("predicate"), new Text(""), new Text("objectsubject"));
    TripleEntry t6 = new TripleEntry(s, new Text(""), new Text("subject"), new Text(""), new Text("predicateobject"));
    TripleEntry t7 = new TripleEntry(s, new Text(""), new Text("subject"), new Text(""), new Text("objectpredicate"));
    TripleEntry t8 = new TripleEntry(p, new Text(""), new Text("predicate"), new Text(""), new Text("subjectobject"));
    TripleEntry t9 = new TripleEntry(o, new Text(""), new Text("object"), new Text(""), new Text("predicatesubject"));

    TripleRowResolver trr = new WholeRowTripleResolver();
    Map<TABLE_LAYOUT,TripleRow> map = trr.serialize(rya);
    System.out.println(map);
    TripleRow tr = map.get(TABLE_LAYOUT.SPO);
    System.out.println("Triple row is" + tr);
    System.out.println("ColumnV is " + tr.getTimestamp());
    byte[] b = new byte[0];
    Key key = new Key(tr.getRow(), tr.getColumnFamily(), tr.getColumnQualifier(), b, 1);
    Value val = new Value(b);

    new MapDriver<Key,Value,CompositeType,TripleCard>().withMapper(new JoinSelectSpoTableOutput.JoinSelectMapper()).withInput(key, val)
        .withOutput(new CompositeType(o, new IntWritable(2)), new TripleCard(t1)).withOutput(new CompositeType(s, new IntWritable(2)), new TripleCard(t2))
        .withOutput(new CompositeType(p, new IntWritable(2)), new TripleCard(t3)).withOutput(new CompositeType(po, new IntWritable(2)), new TripleCard(t6))
        .withOutput(new CompositeType(so, new IntWritable(2)), new TripleCard(t5)).withOutput(new CompositeType(sp, new IntWritable(2)), new TripleCard(t4))
        .withOutput(new CompositeType(op, new IntWritable(2)), new TripleCard(t7)).withOutput(new CompositeType(os, new IntWritable(2)), new TripleCard(t8))
        .withOutput(new CompositeType(ps, new IntWritable(2)), new TripleCard(t9)).runTest();

  }

}
