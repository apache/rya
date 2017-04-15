package org.apache.rya.indexing.pcj.fluo.app.batch;
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
import java.util.UUID;

import org.apache.fluo.api.data.Bytes;
import org.apache.rya.indexing.pcj.fluo.app.IncrementalUpdateConstants;

import com.google.common.base.Preconditions;

/**
 * Class for creating the {@link Byte}s written to the Fluo Row used to identify each {@link BatchInformation}
 * object.  Each Byte row is formed by concatenating a query id and a batch id.   
 *
 */
public class BatchRowKeyUtil {

    /**
     * Creates a Byte row form the query id. The batch id is automatically generated/
     * @param nodeId
     * @return Byte row used to identify the BatchInformation
     */
    public static Bytes getRow(String nodeId) {
        String row = new StringBuilder().append(nodeId).append(IncrementalUpdateConstants.NODEID_BS_DELIM)
                .append(UUID.randomUUID().toString().replace("-", "")).toString();
        return Bytes.of(row);
    }
    
    /**
     * Creates a Byte row from a nodeId and batchId
     * @param nodeId - query node id that batch task will be performed on
     * @param batchId - id used to identify batch
     * @return Byte row used to identify the BatchInformation
     */
    public static Bytes getRow(String nodeId, String batchId) {
        String row = new StringBuilder().append(nodeId).append(IncrementalUpdateConstants.NODEID_BS_DELIM)
                .append(batchId).toString();
        return Bytes.of(row);
    }
    
    /**
     * Given a Byte row, return the query node Id
     * @param row - the Byte row used to identify the BatchInformation
     * @return - the queryId that the batch task is performed on
     */
    public static String getNodeId(Bytes row) {
        String[] stringArray = row.toString().split(IncrementalUpdateConstants.NODEID_BS_DELIM);;
        Preconditions.checkArgument(stringArray.length == 2);
        return stringArray[0];
    }
    
}
