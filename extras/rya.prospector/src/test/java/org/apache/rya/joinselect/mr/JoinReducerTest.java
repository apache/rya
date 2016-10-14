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

import org.apache.rya.joinselect.mr.JoinSelectAggregate;
import org.apache.rya.joinselect.mr.utils.CardList;
import org.apache.rya.joinselect.mr.utils.CardinalityType;
import org.apache.rya.joinselect.mr.utils.CompositeType;
import org.apache.rya.joinselect.mr.utils.TripleCard;
import org.apache.rya.joinselect.mr.utils.TripleEntry;

import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.junit.Test;

public class JoinReducerTest {

  private static final String DELIM = "\u0000";

  @Test
  public void testSingleConstCard() throws InterruptedException, IOException {

    CompositeType ct = new CompositeType("urn:gem:etype#1234", 1);
    TripleEntry te = new TripleEntry("urn:gem#pred", "urn:gem:etype#4567", "predicate", "object", "subject");
    CardinalityType c5 = new CardinalityType(45, "object", 0);
    CardinalityType c1 = new CardinalityType(25, "subject", 2);
    CardinalityType c2 = new CardinalityType(27, "predicate", 2);
    CardinalityType c3 = new CardinalityType(29, "object", 2);
    CardinalityType c4 = new CardinalityType(31, "predicate", 1);
    List<TripleCard> list = new ArrayList<TripleCard>();
    list.add(new TripleCard(c1));
    list.add(new TripleCard(c2));
    list.add(new TripleCard(c3));
    list.add(new TripleCard(c4));
    list.add(new TripleCard(c5));
    list.add(new TripleCard(te));
    System.out.println("List is " + list);

    new ReduceDriver<CompositeType,TripleCard,TripleEntry,CardList>().withReducer(new JoinSelectAggregate.JoinReducer()).withInput(ct, list)
        .withOutput(te, new CardList(25, 31, 45, 0, 0, 0)).runTest();

  }

  @Test
  public void testTwoTripleEntry() throws InterruptedException, IOException {

    CompositeType ct = new CompositeType("urn:gem:etype#1234", 1);
    TripleEntry te1 = new TripleEntry("urn:gem#pred", "urn:gem:etype#4567", "predicate", "object", "subject");
    TripleEntry te2 = new TripleEntry("urn:gem#8910", "urn:gem:etype#4567", "subject", "predicate", "object");
    CardinalityType c5 = new CardinalityType(45, "object", 0);
    CardinalityType c1 = new CardinalityType(25, "subject", 2);
    CardinalityType c2 = new CardinalityType(27, "predicate", 2);
    CardinalityType c3 = new CardinalityType(29, "object", 2);
    CardinalityType c4 = new CardinalityType(31, "predicate", 1);
    List<TripleCard> list = new ArrayList<TripleCard>();
    list.add(new TripleCard(c1));
    list.add(new TripleCard(c2));
    list.add(new TripleCard(c3));
    list.add(new TripleCard(c4));
    list.add(new TripleCard(c5));
    list.add(new TripleCard(te1));
    list.add(new TripleCard(te2));
    System.out.println("List is " + list);

    new ReduceDriver<CompositeType,TripleCard,TripleEntry,CardList>().withReducer(new JoinSelectAggregate.JoinReducer()).withInput(ct, list)
        .withOutput(te1, new CardList(25, 31, 45, 0, 0, 0)).withOutput(te2, new CardList(25, 31, 45, 0, 0, 0)).runTest();

  }

  @Test
  public void testTwoConstCard() throws InterruptedException, IOException {

    CompositeType ct1 = new CompositeType("urn:gem#pred" + DELIM + "urn:gem:etype#1234", 1);
    TripleEntry te1 = new TripleEntry("uri:testSubject", "", "subject", "", "predicateobject");
    TripleEntry te2 = new TripleEntry("uri:testSubject", "", "subject", "", "objectpredicate");

    CardinalityType c5 = new CardinalityType(45, "subjectobject", 0);
    CardinalityType c1 = new CardinalityType(25, "subjectobject", 2);
    CardinalityType c2 = new CardinalityType(27, "predicateobject", 5);
    CardinalityType c3 = new CardinalityType(29, "predicateobject", 2);
    CardinalityType c4 = new CardinalityType(31, "subjectpredicate", 1);
    CardinalityType c6 = new CardinalityType(56, "subjectpredicate", 2);

    List<TripleCard> list1 = new ArrayList<TripleCard>();

    list1.add(new TripleCard(c1));
    list1.add(new TripleCard(c2));
    list1.add(new TripleCard(c3));
    list1.add(new TripleCard(c4));
    list1.add(new TripleCard(c5));
    list1.add(new TripleCard(c6));
    list1.add(new TripleCard(te1));
    list1.add(new TripleCard(te2));

    // System.out.println("List is " + list);

    new ReduceDriver<CompositeType,TripleCard,TripleEntry,CardList>().withReducer(new JoinSelectAggregate.JoinReducer()).withInput(ct1, list1)
        .withOutput(te1, new CardList(0, 0, 0, 31, 29, 45)).withOutput(te2, new CardList(0, 0, 0, 31, 29, 45)).runTest();

  }

}
