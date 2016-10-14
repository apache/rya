package org.apache.rya.joinselect.mr.utils;

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



import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.WritableComparable;

public class CardList implements WritableComparable<CardList> {

  private LongWritable cardS;
  private LongWritable cardP;
  private LongWritable cardO;
  private LongWritable cardSP;
  private LongWritable cardPO;
  private LongWritable cardSO;

  public CardList() {
    cardS = new LongWritable();
    cardP = new LongWritable();
    cardO = new LongWritable();
    cardSP = new LongWritable();
    cardSO = new LongWritable();
    cardPO = new LongWritable();

  }

  public CardList(long cardS, long cardP, long cardO, long cardSP, long cardPO, long cardSO) {
    this.cardS = new LongWritable(cardS);
    this.cardP = new LongWritable(cardP);
    this.cardO = new LongWritable(cardO);
    this.cardSP = new LongWritable(cardSP);
    this.cardSO = new LongWritable(cardSO);
    this.cardPO = new LongWritable(cardPO);
  }

  public CardList(LongWritable cardS, LongWritable cardP, LongWritable cardO, LongWritable cardSP, LongWritable cardPO, LongWritable cardSO) {

    this.cardS = cardS;
    this.cardP = cardP;
    this.cardO = cardO;
    this.cardSP = cardSP;
    this.cardPO = cardPO;
    this.cardSO = cardSO;

  }

  public void setCard(LongWritable cardS, LongWritable cardP, LongWritable cardO, LongWritable cardSP, LongWritable cardPO, LongWritable cardSO) {
    this.cardS = cardS;
    this.cardP = cardP;
    this.cardO = cardO;
    this.cardSP = cardSP;
    this.cardPO = cardPO;
    this.cardSO = cardSO;

  }

  public void setSCard(long cardS) {
    this.cardS = new LongWritable(cardS);
  }

  public void setPCard(long cardP) {
    this.cardP = new LongWritable(cardP);
  }

  public void setOCard(long cardO) {
    this.cardO = new LongWritable(cardO);
  }

  public void setSPCard(long cardSP) {
    this.cardSP = new LongWritable(cardSP);
  }

  public void setSOCard(long cardSO) {
    this.cardSO = new LongWritable(cardSO);
  }

  public void setPOCard(long cardPO) {
    this.cardPO = new LongWritable(cardPO);
  }

  public LongWritable getcardS() {
    return this.cardS;
  }

  public LongWritable getcardP() {
    return this.cardP;
  }

  public LongWritable getcardO() {
    return this.cardO;
  }

  public LongWritable getcardPO() {
    return this.cardPO;
  }

  public LongWritable getcardSO() {
    return this.cardSO;
  }

  public LongWritable getcardSP() {
    return this.cardSP;
  }

  @Override
  public void write(DataOutput out) throws IOException {
    cardS.write(out);
    cardP.write(out);
    cardO.write(out);
    cardSO.write(out);
    cardPO.write(out);
    cardSP.write(out);

  }

  @Override
  public void readFields(DataInput in) throws IOException {
    cardS.readFields(in);
    cardP.readFields(in);
    cardO.readFields(in);
    cardSO.readFields(in);
    cardPO.readFields(in);
    cardSP.readFields(in);

  }

  @Override
  public int hashCode() {
    int result = 7;
    result = result * 17 + cardS.hashCode();
    result = result * 17 + cardP.hashCode();
    result = result * 17 + cardO.hashCode();
    result = result * 17 + cardSP.hashCode();
    result = result * 17 + cardPO.hashCode();
    result = result * 17 + cardSO.hashCode();

    return result;

  }

  @Override
  public boolean equals(Object o) {
    if (o instanceof CardList) {
      CardList comp = (CardList) o;
      return cardS.equals(comp.cardS) && cardP.equals(comp.cardP) && cardO.equals(comp.cardO) && cardSP.equals(comp.cardSP) && cardSO.equals(comp.cardSO)
          && cardPO.equals(comp.cardPO);

    }
    return false;
  }

  @Override
  public String toString() {
    return cardS + "\t" + cardP + "\t" + cardO + "\t" + cardSP + "\t" + cardPO + "\t" + cardSO;

  }

  @Override
  public int compareTo(CardList o) {

    int cmp = cardS.compareTo(o.cardS);
    if (cmp != 0) {
      return cmp;
    }
    cmp = cardP.compareTo(o.cardP);
    if (cmp != 0) {
      return cmp;
    }
    cmp = cardO.compareTo(o.cardO);
    if (cmp != 0) {
      return cmp;
    }
    cmp = cardSP.compareTo(o.cardSP);
    if (cmp != 0) {
      return cmp;
    }
    cmp = cardPO.compareTo(o.cardPO);
    if (cmp != 0) {
      return cmp;
    }

    return cardSO.compareTo(o.cardSO);

  }

}
