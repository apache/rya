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

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

public class CompositeType implements WritableComparable<CompositeType> {

  private Text oldKey;
  private IntWritable priority;

  public CompositeType() {
    oldKey = new Text();
    priority = new IntWritable();
  }

  public CompositeType(String oldKey, int priority) {
    this.oldKey = new Text(oldKey);
    this.priority = new IntWritable(priority);
  }

  public CompositeType(Text oldKey, IntWritable priority) {

    this.oldKey = oldKey;
    this.priority = priority;

  }

  public void setOldKey(Text oldKey) {
    this.oldKey = oldKey;

  }

  public void setPriority(IntWritable priority) {
    this.priority = priority;
  }

  public Text getOldKey() {
    return this.oldKey;
  }

  public IntWritable getPriority() {
    return this.priority;
  }

  @Override
  public void write(DataOutput out) throws IOException {
    oldKey.write(out);
    priority.write(out);

  }

  @Override
  public void readFields(DataInput in) throws IOException {
    oldKey.readFields(in);
    priority.readFields(in);

  }

  @Override
  public int hashCode() {
    int result = 7;
    result = result * 17 + oldKey.hashCode();
    // result = result*17+ priority.hashCode();

    return result;

  }

  @Override
  public boolean equals(Object o) {
    if (o instanceof CompositeType) {
      CompositeType comp = (CompositeType) o;
      return oldKey.equals(comp.oldKey) && priority.equals(comp.priority);

    }
    return false;
  }

  @Override
  public String toString() {
    return oldKey + "\t" + priority;

  }

  @Override
  public int compareTo(CompositeType o) {
    int compare = getOldKey().compareTo(o.getOldKey());
    if (compare != 0) {
      return compare;
    }

    return getPriority().compareTo(o.getPriority());

  }

}
