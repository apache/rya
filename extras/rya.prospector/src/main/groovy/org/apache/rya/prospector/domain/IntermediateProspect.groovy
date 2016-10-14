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

package org.apache.rya.prospector.domain

import org.apache.hadoop.io.WritableComparable

import static org.apache.rya.prospector.domain.TripleValueType.*

/**
 * Date: 12/3/12
 * Time: 11:15 AM
 */
class IntermediateProspect implements WritableComparable<IntermediateProspect> {

    def String index
    def String data
    def String dataType
    def TripleValueType tripleValueType
    def String visibility

    @Override
    int compareTo(IntermediateProspect t) {
        if(!index.equals(t.index))
            return index.compareTo(t.index);
        if(!data.equals(t.data))
            return data.compareTo(t.data);
        if(!dataType.equals(t.dataType))
            return dataType.compareTo(t.dataType);
        if(!tripleValueType.equals(t.tripleValueType))
            return tripleValueType.compareTo(t.tripleValueType);
        if(!visibility.equals(t.visibility))
            return visibility.compareTo(t.visibility);
        return 0
    }

    @Override
    void write(DataOutput dataOutput) {
        dataOutput.writeUTF(index);
        dataOutput.writeUTF(data);
        dataOutput.writeUTF(dataType);
        dataOutput.writeUTF(tripleValueType.name());
        dataOutput.writeUTF(visibility);
    }

    @Override
    void readFields(DataInput dataInput) {
        index = dataInput.readUTF()
        data = dataInput.readUTF()
        dataType = dataInput.readUTF()
        tripleValueType = TripleValueType.valueOf(dataInput.readUTF())
        visibility = dataInput.readUTF()
    }
}
