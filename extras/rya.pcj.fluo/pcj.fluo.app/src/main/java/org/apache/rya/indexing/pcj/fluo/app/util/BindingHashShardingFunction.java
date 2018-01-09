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
package org.apache.rya.indexing.pcj.fluo.app.util;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static org.apache.rya.indexing.pcj.fluo.app.IncrementalUpdateConstants.NODEID_BS_DELIM;
import static org.apache.rya.indexing.pcj.storage.accumulo.BindingSetStringConverter.TYPE_DELIM;

import org.apache.fluo.api.data.Bytes;
import org.apache.fluo.api.data.Bytes.BytesBuilder;
import org.apache.rya.api.domain.RyaType;
import org.apache.rya.api.model.VisibilityBindingSet;
import org.apache.rya.api.resolver.RdfToRyaConversions;
import org.apache.rya.indexing.pcj.storage.accumulo.BindingSetStringConverter;
import org.apache.rya.indexing.pcj.storage.accumulo.VariableOrder;
import org.openrdf.model.Value;

import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.common.hash.Hashing;

/**
 * This class adds or removes a hash to or from the rowId for sharding purposes. When creating a sharded row key, it
 * takes the form: node_prefix:shardId:nodeId//Binding_values. For example, the row key generated from nodeId = SP_123,
 * varOrder = a;b, bs = [a = uri:Bob, b = uri:Doug] would be SP:HASH(uri:Bob):123//uri:Bob;uri:Doug, where HASH(uri:Bob)
 * indicates the shard id hash generated from the Binding value "uri:Bob".
 *
 */
public class BindingHashShardingFunction {

    private static final BindingSetStringConverter BS_CONVERTER = new BindingSetStringConverter();
    private static final int HASH_LEN = 4;

    /**
     * Generates a sharded rowId. The rowId is of the form: node_prefix:shardId:nodeId//Binding_values. For
     * example, the row key generated from nodeId = SP_123, varOrder = a;b, bs = [a = uri:Bob, b = uri:Doug] would be
     * SP:HASH(uri:Bob):123//uri:Bob;uri:Doug, where HASH(uri:Bob) indicates the shard id hash generated from the
     * Binding value "uri:Bob".
     *
     * @param nodeId - Node Id with type and UUID
     * @param varOrder - VarOrder used to order BindingSet values
     * @param bs - BindingSet with partially formed query values
     * @return - serialized Bytes rowId for storing BindingSet results in Fluo
     */
    public static Bytes addShard(String nodeId, VariableOrder varOrder, VisibilityBindingSet bs) {
        checkNotNull(nodeId);
        checkNotNull(varOrder);
        checkNotNull(bs);
        String[] rowPrefixAndId = nodeId.split("_");
        Preconditions.checkArgument(rowPrefixAndId.length == 2);
        String prefix = rowPrefixAndId[0];
        String id = rowPrefixAndId[1];

        String firstBindingString = "";
        Bytes rowSuffix = Bytes.of(id);
        if (varOrder.getVariableOrders().size() > 0) {
            VariableOrder first = new VariableOrder(varOrder.getVariableOrders().get(0));
            firstBindingString = BS_CONVERTER.convert(bs, first);
            rowSuffix = RowKeyUtil.makeRowKey(id, varOrder, bs);
        }

        BytesBuilder builder = Bytes.builder();
        builder.append(Bytes.of(prefix + ":"));
        builder.append(genHash(Bytes.of(id + NODEID_BS_DELIM + firstBindingString)));
        builder.append(":");
        builder.append(rowSuffix);
        return builder.toBytes();
    }

    /**
     * Generates a sharded rowId. The rowId is of the form: node_prefix:shardId:nodeId//Binding_values. For
     * example, the row key generated from nodeId = SP_123, varOrder = a;b, bs = [a = uri:Bob, b = uri:Doug] would be
     * SP:HASH(uri:Bob):123//uri:Bob;uri:Doug, where HASH(uri:Bob) indicates the shard id hash generated from the
     * Binding value "uri:Bob".
     *
     * @param nodeId - Node Id with type and UUID
     * @param firstBsVal - String representation of the first BsValue
     * @return - serialized Bytes prefix for scanning rows
     */
    public static Bytes getShardedScanPrefix(String nodeId, Value firstBsVal) {
        checkNotNull(nodeId);
        checkNotNull(firstBsVal);

        final RyaType ryaValue = RdfToRyaConversions.convertValue(firstBsVal);
        final String bindingString = ryaValue.getData() + TYPE_DELIM + ryaValue.getDataType();

        return getShardedScanPrefix(nodeId, bindingString);
    }

    /**
     * Generates a sharded rowId from the indicated nodeId and bindingString. For example, the row key generated from
     * nodeId = SP_123, varOrder = a;b, bs = [a = uri:Bob, b = uri:Doug] would be
     * SP:HASH(uri:Bob):123//uri:Bob;uri:Doug, where HASH(uri:Bob) indicates the shard id hash generated from the
     * Binding value "uri:Bob".
     *
     * @param nodeId - NodeId with tyep and UUID
     * @param bindingString - String representation of BindingSet values, as formed by {@link BindingSetStringConverter}
     * @return - serialized, sharded Bytes prefix
     */
    public static Bytes getShardedScanPrefix(String nodeId, String bindingString) {
        checkNotNull(nodeId);
        checkNotNull(bindingString);
        String[] rowPrefixAndId = nodeId.split("_");
        Preconditions.checkArgument(rowPrefixAndId.length == 2);
        String prefix = rowPrefixAndId[0];
        String id = rowPrefixAndId[1];

        BytesBuilder builder = Bytes.builder();
        builder.append(prefix + ":");
        builder.append(genHash(Bytes.of(id + NODEID_BS_DELIM + bindingString)));
        builder.append(":");
        builder.append(id);
        builder.append(NODEID_BS_DELIM);
        builder.append(bindingString);
        return builder.toBytes();
    }

    private static boolean hasHash(Bytes prefixBytes, Bytes row) {
        for (int i = prefixBytes.length() + 1; i < prefixBytes.length() + HASH_LEN; i++) {
            byte b = row.byteAt(i);
            boolean isAlphaNum = (b >= 'a' && b <= 'z') || (b >= '0' && b <= '9');
            if (!isAlphaNum) {
                return false;
            }
        }

        if (row.byteAt(prefixBytes.length()) != ':' || row.byteAt(prefixBytes.length() + HASH_LEN + 1) != ':') {
            return false;
        }

        return true;
    }

    /**
     * @return Returns input with prefix and hash stripped from beginning.
     */
    public static Bytes removeHash(Bytes prefixBytes, Bytes row) {
        checkNotNull(prefixBytes);
        checkNotNull(row);
        checkArgument(row.length() >= prefixBytes.length() + 6, "Row is shorter than expected " + row);
        checkArgument(row.subSequence(0, prefixBytes.length()).equals(prefixBytes),
                "Row does not have expected prefix " + row);
        checkArgument(hasHash(prefixBytes, row), "Row does not have expected hash " + row);

        BytesBuilder builder = Bytes.builder();
        builder.append(prefixBytes);
        builder.append("_");
        builder.append(row.subSequence(prefixBytes.length() + 6, row.length()));
        return builder.toBytes();
    }

    private static String genHash(Bytes row) {
        int hash = Hashing.murmur3_32().hashBytes(row.toArray()).asInt();
        hash = hash & 0x7fffffff;
        // base 36 gives a lot more bins in 4 bytes than hex, but it is still human readable which is
        // nice for debugging.
        String hashString = Strings.padStart(Integer.toString(hash, Character.MAX_RADIX), HASH_LEN, '0');
        hashString = hashString.substring(hashString.length() - HASH_LEN);
        return hashString;
    }
}
