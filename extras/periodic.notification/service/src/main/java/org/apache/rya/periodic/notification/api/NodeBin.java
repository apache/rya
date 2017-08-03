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
package org.apache.rya.periodic.notification.api;

import java.util.Objects;

/**
 * Object used to indicate the id of a given Periodic Query
 * along with a particular bin of results.  This Object is used
 * by the {@link BinPruner} to clean up old query results after
 * they have been processed.
 *
 */
public class NodeBin {

    private long bin;
    private String nodeId;

    public NodeBin(String nodeId, long bin) {
        this.bin = bin;
        this.nodeId = nodeId;
    }

    /**
     * @return id of Periodic Query
     */
    public String getNodeId() {
        return nodeId;
    }
/**
 * @return bin id of results for a given Periodic Query 
 */
    public long getBin() {
        return bin;
    }

    @Override
    public boolean equals(Object other) {
        if (this == other) {
            return true;
        }

        if (other instanceof NodeBin) {
            NodeBin bin = (NodeBin) other;
            return this.bin == bin.bin && this.nodeId.equals(bin.nodeId);
        }

        return false;
    }

    @Override
    public int hashCode() {
        return Objects.hash(bin, nodeId);
    }

    @Override
    public String toString() {
        return new StringBuilder().append("Node Bin \n").append("   QueryId: " + nodeId + "\n").append("   Bin: " + bin + "\n").toString();
    }

}
