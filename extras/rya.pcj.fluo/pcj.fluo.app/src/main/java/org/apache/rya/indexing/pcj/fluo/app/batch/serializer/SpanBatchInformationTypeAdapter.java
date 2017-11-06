package org.apache.rya.indexing.pcj.fluo.app.batch.serializer;

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
import java.lang.reflect.Type;
import java.util.Optional;

import org.apache.fluo.api.data.Column;
import org.apache.fluo.api.data.RowColumn;
import org.apache.fluo.api.data.Span;
import org.apache.rya.indexing.pcj.fluo.app.batch.SpanBatchDeleteInformation;

import com.google.gson.JsonDeserializationContext;
import com.google.gson.JsonDeserializer;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParseException;
import com.google.gson.JsonPrimitive;
import com.google.gson.JsonSerializationContext;
import com.google.gson.JsonSerializer;

/**
 * JsonSerializer/JsonDeserializer used to serialize/deserialize {@link SpanBatchDeleteInformation} objects.
 *
 */
public class SpanBatchInformationTypeAdapter
        implements JsonSerializer<SpanBatchDeleteInformation>, JsonDeserializer<SpanBatchDeleteInformation> {

    @Override
    public SpanBatchDeleteInformation deserialize(JsonElement element, Type typeOfT, JsonDeserializationContext context)
            throws JsonParseException {
        JsonObject json = element.getAsJsonObject();
        int batchSize = json.get("batchSize").getAsInt();
        String[] colArray = json.get("column").getAsString().split("\u0000");
        Column column = new Column(colArray[0], colArray[1]);
        String[] rows = json.get("span").getAsString().split("\u0000");
        boolean startInc = json.get("startInc").getAsBoolean();
        boolean endInc = json.get("endInc").getAsBoolean();
        Span span = new Span(new RowColumn(rows[0]), startInc, new RowColumn(rows[1]), endInc);
        String nodeId = json.get("nodeId").getAsString();
        Optional<String> id = Optional.empty();
        if (!nodeId.isEmpty()) {
            id = Optional.of(nodeId);
        }
        return SpanBatchDeleteInformation.builder().setNodeId(id).setBatchSize(batchSize).setSpan(span).setColumn(column).build();
    }

    @Override
    public JsonElement serialize(SpanBatchDeleteInformation batch, Type typeOfSrc, JsonSerializationContext context) {
        JsonObject result = new JsonObject();
        result.add("class", new JsonPrimitive(batch.getClass().getName()));
        result.add("batchSize", new JsonPrimitive(batch.getBatchSize()));
        Column column = batch.getColumn();
        result.add("column", new JsonPrimitive(column.getsFamily() + "\u0000" + column.getsQualifier()));
        Span span = batch.getSpan();
        result.add("span", new JsonPrimitive(span.getStart().getsRow() + "\u0000" + span.getEnd().getsRow()));
        result.add("startInc", new JsonPrimitive(span.isStartInclusive()));
        result.add("endInc", new JsonPrimitive(span.isEndInclusive()));
        String nodeId = batch.getNodeId().orElse("");
        result.add("nodeId", new JsonPrimitive(nodeId));
        return result;
    }

}
