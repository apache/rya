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
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

public class CardinalityType implements WritableComparable<CardinalityType> {

  private LongWritable card;
  private Text cardType;
  private LongWritable ts;

  public CardinalityType() {
    card = new LongWritable();
    cardType = new Text();
    ts = new LongWritable();
  }

  public CardinalityType(int card, String cardType, long ts) {

    this.card = new LongWritable(card);
    this.cardType = new Text(cardType);
    this.ts = new LongWritable(ts);

  }

  public CardinalityType(LongWritable card, Text cardType, LongWritable ts) {

    this.card = card;
    this.ts = ts;
    this.cardType = cardType;

  }

  public void set(CardinalityType ct) {
    this.card.set(ct.card.get());
    this.ts.set(ct.ts.get());
    this.cardType.set(ct.cardType);
  }

  public void setCard(LongWritable card) {
    this.card = card;

  }

  public void setCardType(Text cardType) {
    this.cardType = cardType;
  }

  public void setTS(LongWritable ts) {
    this.ts = ts;
  }

  public LongWritable getCard() {
    return this.card;
  }

  public Text getCardType() {
    return this.cardType;
  }

  public LongWritable getTS() {
    return this.ts;
  }

  @Override
  public void write(DataOutput out) throws IOException {
    card.write(out);
    cardType.write(out);
    ts.write(out);

  }

  @Override
  public void readFields(DataInput in) throws IOException {
    card.readFields(in);
    cardType.readFields(in);
    ts.readFields(in);

  }

  @Override
  public int hashCode() {
    int result = 7;
    result = result * 17 + card.hashCode();
    result = result * 17 + cardType.hashCode();
    result = result * 17 + ts.hashCode();

    return result;

  }

  @Override
  public boolean equals(Object o) {
    if (o instanceof CardinalityType) {
      CardinalityType trip = (CardinalityType) o;
      return card.equals(trip.card) && cardType.equals(trip.cardType) && ts.equals(trip.ts);

    }
    return false;
  }

  @Override
  public String toString() {
    return card + "   " + cardType + "   " + ts;

  }

  @Override
  public int compareTo(CardinalityType o) {

    int cmp = cardType.compareTo(o.cardType);
    if (cmp != 0) {
      return cmp;
    }
    cmp = ts.compareTo(o.ts);
    if (cmp != 0) {
      return cmp;
    }
    return card.compareTo(o.card);

  }

}
