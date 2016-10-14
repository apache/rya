package org.apache.rya.accumulo.mr.merge.reducers;

/*
 * #%L
 * org.apache.rya.accumulo.mr.merge
 * %%
 * Copyright (C) 2014 Rya
 * %%
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 * #L%
 */

import java.io.IOException;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

import org.apache.rya.accumulo.mr.merge.util.GroupedRow;

/**
 * Outputs rows to different files according to their associated group names, for use with {@link AccumuloFileOutputFormat}.
 */
public class MultipleFileReducer extends Reducer<GroupedRow, GroupedRow, Key, Value> {
    private MultipleOutputs<Key, Value> mos;

    @Override
    protected void setup(Context context) {
        mos = new MultipleOutputs<>(context);
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        if (mos != null) {
            mos.close();
        }
    }

    /**
     * Writes <{@link Key}, {@link Value}> pairs to a file, where the path to the output file is determined by the group.
     * @param   group   Contains the group name (a String) which is used to route output to the appropriate subdirectory
     * @param   rows    Contain the actual Accumulo rows to be written
     * @param   context Context for writing
     */
    @Override
    protected void reduce(GroupedRow group, Iterable<GroupedRow> rows, Context context) throws IOException, InterruptedException {
        String groupName = group.getGroup().toString();
        String destination = groupName + "/files/part";
        for (GroupedRow row : rows) {
            mos.write(row.getKey(), row.getValue(), destination);
        }
    }
}
