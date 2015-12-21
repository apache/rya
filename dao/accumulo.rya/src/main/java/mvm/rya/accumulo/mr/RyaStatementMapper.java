  
package mvm.rya.accumulo.mr;
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
import static mvm.rya.api.RdfCloudTripleStoreConstants.TBL_OSP_SUFFIX;
import static mvm.rya.api.RdfCloudTripleStoreConstants.TBL_PO_SUFFIX;
import static mvm.rya.api.RdfCloudTripleStoreConstants.TBL_PRFX_DEF;
import static mvm.rya.api.RdfCloudTripleStoreConstants.TBL_SPO_SUFFIX;

import java.io.IOException;
import java.util.Collection;
import java.util.Map;

import org.apache.accumulo.core.data.Mutation;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import mvm.rya.accumulo.AccumuloRdfConfiguration;
import mvm.rya.accumulo.RyaTableMutationsFactory;
import mvm.rya.accumulo.mr.utils.MRUtils;
import mvm.rya.api.RdfCloudTripleStoreConstants.TABLE_LAYOUT;
import mvm.rya.api.resolver.RyaTripleContext;

public class RyaStatementMapper extends Mapper<Text, RyaStatementWritable, Text, Mutation> {

    private Text spoTable;
    private Text poTable;
    private Text ospTable;
    private RyaTableMutationsFactory mutationsFactory;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        super.setup(context);

        String tablePrefix = context.getConfiguration().get(MRUtils.TABLE_PREFIX_PROPERTY, TBL_PRFX_DEF);
        spoTable = new Text(tablePrefix + TBL_SPO_SUFFIX);
        poTable = new Text(tablePrefix + TBL_PO_SUFFIX);
        ospTable = new Text(tablePrefix + TBL_OSP_SUFFIX);

        RyaTripleContext ryaContext = RyaTripleContext.getInstance(new AccumuloRdfConfiguration(context.getConfiguration()));
        mutationsFactory = new RyaTableMutationsFactory(ryaContext);
    }

    @Override
    protected void map(Text key, RyaStatementWritable value, Context context) throws IOException, InterruptedException {

        Map<TABLE_LAYOUT, Collection<Mutation>> mutations = mutationsFactory.serialize(value.getRyaStatement());

        for(TABLE_LAYOUT layout : mutations.keySet()) {

            Text table = null;
            switch (layout) {
                case SPO:
                    table = spoTable;
                    break;
                case OSP:
                    table = ospTable;
                    break;
                case PO:
                    table = poTable;
                    break;
            }

            for(Mutation mutation : mutations.get(layout)) {
                context.write(table, mutation);
            }
        }
    }
}