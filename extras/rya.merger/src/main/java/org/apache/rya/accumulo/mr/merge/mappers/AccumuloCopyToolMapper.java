package mvm.rya.accumulo.mr.merge.mappers;

/*
 * #%L
 * mvm.rya.accumulo.mr.merge
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
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Value;
import org.apache.hadoop.io.Text;

/**
 * Extended {@link BaseCopyToolMapper} that handles the {@code AccumuloOutputFormat} for the copy tool.
 */
public class AccumuloCopyToolMapper extends BaseCopyToolMapper<Key, Value, Text, Mutation> {
    /**
     * Creates a new {@link AccumuloCopyToolMapper}.
     */
    public AccumuloCopyToolMapper() {
    }

    @Override
    protected void map(Key key, Value value, Context context) throws IOException, InterruptedException {
        //log.trace("Mapping key: " + key + " = " + value);
        Mutation mutation = makeAddMutation(key, value);
        context.write(childTableNameText, mutation);
    }

    private static Mutation makeAddMutation(Key key, Value value) {
        Mutation mutation = new Mutation(key.getRow().getBytes());
        mutation.put(key.getColumnFamily(), key.getColumnQualifier(), key.getColumnVisibilityParsed(), key.getTimestamp(), value);
        return mutation;
    }
}