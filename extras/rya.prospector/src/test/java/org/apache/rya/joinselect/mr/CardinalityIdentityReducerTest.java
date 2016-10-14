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
import java.util.ArrayList;
import java.util.List;

import org.apache.rya.joinselect.mr.utils.CardList;
import org.apache.rya.joinselect.mr.utils.TripleEntry;

import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Value;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.junit.Test;

public class CardinalityIdentityReducerTest {

  private static final String DELIM = "\u0000";

  @Test
  public void testCIReducerOneConstant() throws InterruptedException, IOException {

    TripleEntry te = new TripleEntry(new Text("urn:gem:etype#1234"), new Text(""), new Text("subject"), new Text(""), new Text("object"));
    CardList cL1 = new CardList(1, 2, 3, 0, 0, 0);
    CardList cL2 = new CardList(4, 5, 6, 0, 0, 0);
    CardList cl = new CardList(5, 7, 9, 0, 0, 0);
    List<CardList> list = new ArrayList<CardList>();
    list.add(cL1);
    list.add(cL2);

    Text row = new Text(te.getFirstPos().toString() + DELIM + te.getFirst().toString());
    Mutation m1 = new Mutation(row);
    m1.put(new Text(te.getKeyPos().toString() + "subject"), new Text(cl.getcardS().toString()), new Value(new byte[0]));
    Mutation m2 = new Mutation(row);
    m2.put(new Text(te.getKeyPos().toString() + "predicate"), new Text(cl.getcardP().toString()), new Value(new byte[0]));
    Mutation m3 = new Mutation(row);
    m3.put(new Text(te.getKeyPos().toString() + "object"), new Text(cl.getcardO().toString()), new Value(new byte[0]));
    Text table = new Text("");

    new ReduceDriver<TripleEntry,CardList,Text,Mutation>().withReducer(new JoinSelectStatisticsSum.CardinalityIdentityReducer()).withInput(te, list)
        .withOutput(table, m1).withOutput(table, m2).withOutput(table, m3).runTest();

  }

  @Test
  public void testCIReducerTwoConstant() throws InterruptedException, IOException {

    TripleEntry te = new TripleEntry(new Text("urn:gem:etype#1234"), new Text("urn:gem#pred"), new Text("subject"), new Text("predicate"), new Text("object"));
    CardList cL1 = new CardList(1, 2, 3, 0, 0, 0);
    CardList cL2 = new CardList(4, 5, 6, 0, 0, 0);
    CardList cl = new CardList(5, 7, 9, 0, 0, 0);
    List<CardList> list = new ArrayList<CardList>();
    list.add(cL1);
    list.add(cL2);

    Text row = new Text(te.getFirstPos().toString() + te.getSecondPos().toString() + DELIM + te.getFirst().toString() + DELIM + te.getSecond());
    Mutation m1 = new Mutation(row);
    m1.put(new Text(te.getKeyPos().toString() + "subject"), new Text(cl.getcardS().toString()), new Value(new byte[0]));
    Mutation m2 = new Mutation(row);
    m2.put(new Text(te.getKeyPos().toString() + "predicate"), new Text(cl.getcardP().toString()), new Value(new byte[0]));
    Mutation m3 = new Mutation(row);
    m3.put(new Text(te.getKeyPos().toString() + "object"), new Text(cl.getcardO().toString()), new Value(new byte[0]));
    Text table = new Text("");

    new ReduceDriver<TripleEntry,CardList,Text,Mutation>().withReducer(new JoinSelectStatisticsSum.CardinalityIdentityReducer()).withInput(te, list)
        .withOutput(table, m1).withOutput(table, m2).withOutput(table, m3).runTest();

  }

  @Test
  public void testJoinTwoVars() throws InterruptedException, IOException {

    TripleEntry te = new TripleEntry(new Text("urn:gem:etype#1234"), new Text(""), new Text("subject"), new Text(""), new Text("predicateobject"));
    CardList cL1 = new CardList(0, 0, 0, 1, 2, 3);
    CardList cL2 = new CardList(0, 0, 0, 4, 5, 6);
    CardList cl = new CardList(0, 0, 0, 5, 7, 9);
    List<CardList> list = new ArrayList<CardList>();
    list.add(cL1);
    list.add(cL2);

    Text row = new Text(te.getFirstPos().toString() + DELIM + te.getFirst().toString());
    Mutation m1 = new Mutation(row);
    m1.put(new Text(te.getKeyPos().toString() + "subjectpredicate"), new Text(cl.getcardSP().toString()), new Value(new byte[0]));
    Mutation m2 = new Mutation(row);
    m2.put(new Text(te.getKeyPos().toString() + "predicateobject"), new Text(cl.getcardPO().toString()), new Value(new byte[0]));
    Mutation m3 = new Mutation(row);
    m3.put(new Text(te.getKeyPos().toString() + "objectsubject"), new Text(cl.getcardSO().toString()), new Value(new byte[0]));
    Text table = new Text("");

    new ReduceDriver<TripleEntry,CardList,Text,Mutation>().withReducer(new JoinSelectStatisticsSum.CardinalityIdentityReducer()).withInput(te, list)
        .withOutput(table, m1).withOutput(table, m2).withOutput(table, m3).runTest();

  }

  @Test
  public void testJoinTwoVarsReverseOrder() throws InterruptedException, IOException {

    TripleEntry te = new TripleEntry(new Text("urn:gem:etype#1234"), new Text(""), new Text("subject"), new Text(""), new Text("objectpredicate"));
    CardList cL1 = new CardList(0, 0, 0, 1, 2, 3);
    CardList cL2 = new CardList(0, 0, 0, 4, 5, 6);
    CardList cl = new CardList(0, 0, 0, 5, 7, 9);
    List<CardList> list = new ArrayList<CardList>();
    list.add(cL1);
    list.add(cL2);

    Text row = new Text(te.getFirstPos().toString() + DELIM + te.getFirst().toString());
    Mutation m1 = new Mutation(row);
    m1.put(new Text("predicateobject" + "predicatesubject"), new Text(cl.getcardSP().toString()), new Value(new byte[0]));
    Mutation m2 = new Mutation(row);
    m2.put(new Text("predicateobject" + "objectpredicate"), new Text(cl.getcardPO().toString()), new Value(new byte[0]));
    Mutation m3 = new Mutation(row);
    m3.put(new Text("predicateobject" + "subjectobject"), new Text(cl.getcardSO().toString()), new Value(new byte[0]));
    Text table = new Text("");

    new ReduceDriver<TripleEntry,CardList,Text,Mutation>().withReducer(new JoinSelectStatisticsSum.CardinalityIdentityReducer()).withInput(te, list)
        .withOutput(table, m1).withOutput(table, m2).withOutput(table, m3).runTest();

  }

}
