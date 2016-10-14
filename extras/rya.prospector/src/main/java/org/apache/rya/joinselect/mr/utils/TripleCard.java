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

import org.apache.hadoop.io.WritableComparable;

public class TripleCard implements WritableComparable<TripleCard> {

  private CardinalityType card = null;
  private TripleEntry te = null;

  private CardinalityType tempCard = new CardinalityType();
  private TripleEntry tempTe = new TripleEntry();

  public TripleCard() {}

  public TripleCard(CardinalityType card) {
    this.setCard(card);
  }

  public TripleCard(TripleEntry te) {
    this.setTE(te);
  }

  public void setCard(CardinalityType card) {
    tempCard.set(card);
    this.card = tempCard;
    this.te = null;
  }

  public void setTE(TripleEntry te) {
    tempTe.setTE(te);
    this.te = tempTe;
    this.card = null;
  }

  public CardinalityType getCard() {
    return this.card;
  }

  public TripleEntry getTE() {
    return this.te;
  }

  public boolean isCardNull() {
    return (card == null);
  }

  public boolean isTeNull() {
    return (te == null);
  }

  @Override
  public void write(DataOutput out) throws IOException {
    if (card != null) {
      out.writeBoolean(true);
      card.write(out);
    } else {
      out.writeBoolean(false);
      te.write(out);
    }

  }

  @Override
  public void readFields(DataInput in) throws IOException {
    if (in.readBoolean()) {
      tempCard.readFields(in);
      card = tempCard;
      te = null;
    } else {
      tempTe.readFields(in);
      te = tempTe;
      card = null;
    }
  }

  @Override
  public int hashCode() {
    int result = 7;
    if (card != null) {
      result = result * 17 + card.hashCode();
    } else {
      result = result * 17 + te.hashCode();
    }
    return result;

  }

  @Override
  public boolean equals(Object o) {
    if (o instanceof TripleCard) {
      TripleCard comp = (TripleCard) o;
      if (card != null) {
        return card.equals(comp.card);
      } else {
        return te.equals(comp.te);
      }
    }
    return false;
  }

  @Override
  public String toString() {
    if (card != null) {
      return card.toString();
    } else {
      return te.toString();
    }
  }

  @Override
  public int compareTo(TripleCard o) {

    if (card != null) {
      return card.compareTo(o.card);
    } else {
      return te.compareTo(o.te);
    }
  }

}
