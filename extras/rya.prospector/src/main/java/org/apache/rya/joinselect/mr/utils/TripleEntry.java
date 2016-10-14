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

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

public class TripleEntry implements WritableComparable<TripleEntry> {

  private Text first;
  private Text second;
  private Text firstPos;
  private Text secondPos;
  private Text keyPos;

  public TripleEntry() {

    first = new Text();
    second = new Text();
    firstPos = new Text();
    secondPos = new Text();
    keyPos = new Text();

  }

  public TripleEntry(String first, String second, String firstPos, String secondPos, String keyPos) {
    this.first = new Text(first);
    this.second = new Text(second);
    this.firstPos = new Text(firstPos);
    this.secondPos = new Text(secondPos);
    this.keyPos = new Text(keyPos);
  }

  public TripleEntry(Text first, Text second, Text firstPos, Text secondPos, Text keyPos) {
    this.first = first;
    this.second = second;
    this.firstPos = firstPos;
    this.secondPos = secondPos;
    this.keyPos = keyPos;
  }

  public void setEntry(Text first, Text second) {
    this.first = first;
    this.second = second;
  }

  public void setPosition(Text firstPos, Text secondPos, Text keyPos) {
    this.firstPos = firstPos;
    this.secondPos = secondPos;
    this.keyPos = keyPos;
  }

  public void setTE(TripleEntry te) {

    this.first.set(te.first);
    this.second.set(te.second);
    this.firstPos.set(te.firstPos);
    this.secondPos.set(te.secondPos);
    this.keyPos.set(te.keyPos);

  }

  public Text getFirst() {
    return this.first;
  }

  public Text getSecond() {
    return this.second;
  }

  public Text getFirstPos() {
    return this.firstPos;
  }

  public Text getSecondPos() {
    return this.secondPos;
  }

  public Text getKeyPos() {
    return this.keyPos;
  }

  @Override
  public void write(DataOutput out) throws IOException {
    first.write(out);
    second.write(out);
    firstPos.write(out);
    secondPos.write(out);
    keyPos.write(out);

  }

  @Override
  public void readFields(DataInput in) throws IOException {
    first.readFields(in);
    second.readFields(in);
    firstPos.readFields(in);
    secondPos.readFields(in);
    keyPos.readFields(in);

  }

  @Override
  public int hashCode() {
    int result = 7;
    result = result * 17 + first.hashCode();
    result = result * 17 + second.hashCode();
    result = result * 17 + firstPos.hashCode();
    result = result * 17 + secondPos.hashCode();
    result = result * 17 + keyPos.hashCode();

    return result;

  }

  @Override
  public boolean equals(Object o) {
    if (o instanceof TripleEntry) {
      TripleEntry trip = (TripleEntry) o;
      return first.equals(trip.first) && second.equals(trip.second) && firstPos.equals(trip.firstPos) && secondPos.equals(trip.secondPos)
          && keyPos.equals(trip.keyPos);

    }
    return false;
  }

  @Override
  public String toString() {
    return first + "\t" + firstPos + "\t" + second + "\t" + secondPos + "\t" + keyPos;

  }

  @Override
  public int compareTo(TripleEntry o) {

    int cmp = first.compareTo(o.first);
    if (cmp != 0) {
      return cmp;
    }
    cmp = firstPos.compareTo(o.firstPos);
    if (cmp != 0) {
      return cmp;
    }
    cmp = second.compareTo(o.second);
    if (cmp != 0) {
      return cmp;
    }

    cmp = secondPos.compareTo(o.secondPos);
    if (cmp != 0) {
      return cmp;
    }
    return keyPos.compareTo(o.keyPos);

  }

}
