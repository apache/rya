package org.apache.rya.accumulo.mr.merge.mappers;

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
import java.util.Map;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.log4j.Logger;

import org.apache.rya.accumulo.AccumuloRdfUtils;
import org.apache.rya.accumulo.mr.merge.util.AccumuloRyaUtils;
import org.apache.rya.api.RdfCloudTripleStoreConstants.TABLE_LAYOUT;
import org.apache.rya.api.domain.RyaStatement;
import org.apache.rya.api.resolver.triple.TripleRow;
import org.apache.rya.api.resolver.triple.TripleRowResolverException;

/**
 * Extended {@link BaseCopyToolMapper} that handles the {@code AccumuloFileOutputFormat} for the copy tool.
 */
public class FileCopyToolMapper extends BaseCopyToolMapper<Key, Value, Key, Value> {
    private static final Logger log = Logger.getLogger(FileCopyToolMapper.class);

    /**
     * Creates a new {@link FileCopyToolMapper}.
     */
    public FileCopyToolMapper() {
    }

    @Override
    protected void addMetadataKeys(Context context) throws IOException {
        try {
            if (runTime != null) {
                log.info("Writing copy tool run time metadata to child table: " + runTime);
                RyaStatement ryaStatement = AccumuloRyaUtils.createCopyToolRunTimeRyaStatement(runTime);
                writeRyaStatement(ryaStatement, context);
            }

            if (startTime != null) {
                log.info("Writing copy split time metadata to child table: " + startTime);
                RyaStatement ryaStatement = AccumuloRyaUtils.createCopyToolSplitTimeRyaStatement(startTime);
                writeRyaStatement(ryaStatement, context);
            }

            if (timeOffset != null) {
                log.info("Writing copy tool time offset metadata to child table: " + timeOffset);
                RyaStatement ryaStatement = AccumuloRyaUtils.createTimeOffsetRyaStatement(timeOffset);
                writeRyaStatement(ryaStatement, context);
            }
        } catch (TripleRowResolverException | IOException | InterruptedException e) {
            throw new IOException("Failed to write metadata key", e);
        }
    }

    private void writeRyaStatement(RyaStatement ryaStatement, Context context) throws TripleRowResolverException, IOException, InterruptedException {
        Map<TABLE_LAYOUT, TripleRow> serialize = childRyaContext.getTripleResolver().serialize(ryaStatement);
        TripleRow tripleRow = serialize.get(TABLE_LAYOUT.SPO);
        Key key = AccumuloRdfUtils.from(tripleRow);
        Value value = AccumuloRdfUtils.extractValue(tripleRow);
        context.write(key, value);
    }
}