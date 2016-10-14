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

package org.apache.rya.prospector.service

import org.apache.rya.prospector.utils.ProspectorUtils
import org.apache.accumulo.core.data.Key
import org.apache.accumulo.core.data.Range
import org.apache.accumulo.core.security.Authorizations
import org.apache.hadoop.io.Text

import static org.apache.rya.prospector.utils.ProspectorConstants.METADATA
import static org.apache.rya.prospector.utils.ProspectorConstants.PROSPECT_TIME
import org.apache.rya.prospector.plans.IndexWorkPlanManager
import org.apache.rya.prospector.plans.impl.ServicesBackedIndexWorkPlanManager
import org.apache.rya.prospector.plans.IndexWorkPlan
import org.apache.rya.prospector.domain.IndexEntry

/**
 * Date: 12/5/12
 * Time: 12:28 PM
 */
class ProspectorService {

    def connector
    String tableName

    IndexWorkPlanManager manager = new ServicesBackedIndexWorkPlanManager()
    Map<String, IndexWorkPlan> plans

    ProspectorService(def connector, String tableName) {
        this.connector = connector
        this.tableName = tableName
        this.plans = ProspectorUtils.planMap(manager.plans)

        //init
        def tos = connector.tableOperations()
        if(!tos.exists(tableName)) {
            tos.create(tableName)
        }
    }

    public Iterator<Long> getProspects(String[] auths) {

        def scanner = connector.createScanner(tableName, new Authorizations(auths))
        scanner.setRange(Range.exact(METADATA));
        scanner.fetchColumnFamily(new Text(PROSPECT_TIME));

        def iterator = scanner.iterator();

        return new Iterator<Long>() {


            @Override
            public boolean hasNext() {
                return iterator.hasNext();
            }

            @Override
            public Long next() {
                return iterator.next().getKey().getTimestamp();
            }

            @Override
            public void remove() {
                iterator.remove();
            }
        };

    }

    public Iterator<Long> getProspectsInRange(long beginTime, long endTime, String[] auths) {

        def scanner = connector.createScanner(tableName, new Authorizations(auths))
        scanner.setRange(new Range(
                new Key(METADATA, PROSPECT_TIME, ProspectorUtils.getReverseIndexDateTime(new Date(endTime)), "", Long.MAX_VALUE),
                new Key(METADATA, PROSPECT_TIME, ProspectorUtils.getReverseIndexDateTime(new Date(beginTime)), "", 0l)
        ))
        def iterator = scanner.iterator();

        return new Iterator<Long>() {

            @Override
            public boolean hasNext() {
                return iterator.hasNext();
            }

            @Override
            public Long next() {
                return iterator.next().getKey().getTimestamp();
            }

            @Override
            public void remove() {
                iterator.remove();
            }
        };

    }

    public List<IndexEntry> query(List<Long> prospectTimes, String indexType, String type, List<String> index, String dataType, String[] auths) {
        assert indexType != null

        def plan = plans.get(indexType)
        assert plan != null: "Index Type: ${indexType} does not exist"
		String compositeIndex = plan.getCompositeValue(index);

        return plan.query(connector, tableName, prospectTimes, type, compositeIndex, dataType, auths)
    }
}
