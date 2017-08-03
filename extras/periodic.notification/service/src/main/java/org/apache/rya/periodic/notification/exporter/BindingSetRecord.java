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
package org.apache.rya.periodic.notification.exporter;

import org.openrdf.query.BindingSet;

import com.google.common.base.Objects;

/**
 * Object that associates a {@link BindingSet} with a given Kafka topic.
 * This ensures that the {@link KafkaPeriodicBindingSetExporter} can export
 * each BindingSet to its appropriate topic.
 *
 */
public class BindingSetRecord {

    private BindingSet bs;
    private String topic;
    
    public BindingSetRecord(BindingSet bs, String topic) {
        this.bs = bs;
        this.topic = topic;
    }
    
    /**
     * @return BindingSet in this BindingSetRecord
     */
    public BindingSet getBindingSet() {
        return bs;
    }
    
    /**
     * @return Kafka topic for this BindingSetRecord
     */
    public String getTopic() {
        return topic;
    }
    
    @Override 
    public boolean equals(Object o) {
        if(this == o) {
            return true;
        }
        
        if(o instanceof BindingSetRecord) {
            BindingSetRecord record = (BindingSetRecord) o;
            return Objects.equal(this.bs, record.bs)&&Objects.equal(this.topic,record.topic);
        }
        
        return false;
    }
    
    @Override
    public int hashCode() {
        return Objects.hashCode(bs, topic);
    }
    
    @Override
    public String toString() {
        return new StringBuilder().append("Binding Set Record \n").append("  Topic: " + topic + "\n").append("  BindingSet: " + bs + "\n")
                .toString();
    }
    
}
