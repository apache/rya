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
package org.apache.rya.indexing.pcj.storage.accumulo;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

import javax.annotation.ParametersAreNonnullByDefault;

import com.google.common.collect.Lists;

/**
 * Shifts the variables to the left so that each variable will appear at
 * the head of the varOrder once.
 */
@ParametersAreNonnullByDefault
public class ShiftVarOrderFactory implements PcjVarOrderFactory {
    @Override
    public Set<VariableOrder> makeVarOrders(final VariableOrder varOrder) {
        final Set<VariableOrder> varOrders = new HashSet<>();

        final List<String> cyclicBuff = Lists.newArrayList( varOrder.getVariableOrders() );
        final String[] varOrderBuff = new String[ cyclicBuff.size() ];

        for(int shift = 0; shift < cyclicBuff.size(); shift++) {
            // Build a variable order.
            for(int i = 0; i < cyclicBuff.size(); i++) {
                varOrderBuff[i] = cyclicBuff.get(i);
            }
            varOrders.add( new VariableOrder(varOrderBuff) );

            // Shift the order the variables will appear in the cyclic buffer.
            cyclicBuff.add( cyclicBuff.remove(0) );
        }

        return varOrders;
    }
}