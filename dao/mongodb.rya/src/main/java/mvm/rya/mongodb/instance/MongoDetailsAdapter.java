package mvm.rya.mongodb.instance;

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

import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map.Entry;

import javax.annotation.ParametersAreNonnullByDefault;

import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import com.mongodb.BasicDBList;
import com.mongodb.BasicDBObject;
import com.mongodb.BasicDBObjectBuilder;
import com.mongodb.DBObject;

import mvm.rya.api.instance.RyaDetails;
import mvm.rya.api.instance.RyaDetails.EntityCentricIndexDetails;
import mvm.rya.api.instance.RyaDetails.FreeTextIndexDetails;
import mvm.rya.api.instance.RyaDetails.GeoIndexDetails;
import mvm.rya.api.instance.RyaDetails.JoinSelectivityDetails;
import mvm.rya.api.instance.RyaDetails.PCJIndexDetails;
import mvm.rya.api.instance.RyaDetails.PCJIndexDetails.FluoDetails;
import mvm.rya.api.instance.RyaDetails.PCJIndexDetails.PCJDetails;
import mvm.rya.api.instance.RyaDetails.PCJIndexDetails.PCJDetails.PCJUpdateStrategy;
import mvm.rya.api.instance.RyaDetails.ProspectorDetails;
import mvm.rya.api.instance.RyaDetails.TemporalIndexDetails;

/**
 * Serializes configuration details for use in Mongo.
 * The {@link DBObject} will look like:
 * <pre>
 * {@code
 * {
 *   "instanceName": &lt;string&gt;,
 *   "version": &lt;string&gt;?,
 *   "entityCentricDetails": &lt;boolean&gt;,
 *   "geoDetails": &lt;boolean&gt;,
 *   "pcjDetails": {
 *       "enabled": &lt;boolean&gt;,
 *       "fluoName": &lt;string&gt;,
 *       "pcjs": [{
 *           "id": &lt;string&gt;,
 *           "updateStrategy": &lt;string&gt;,
 *           "lastUpdate": &lt;date&gt;
 *         },...,{}
 *       ]
 *   },
 *   "temporalDetails": &lt;boolean&gt;,
 *   "freeTextDetails": &lt;boolean&gt;,
 *   "prospectorDetails": &lt;date&gt;,
 *   "joinSelectivityDetails": &lt;date&gt;
 * }
 * </pre>
 */
@ParametersAreNonnullByDefault
public class MongoDetailsAdapter {
    public static final String INSTANCE_KEY = "instanceName";
    public static final String VERSION_KEY = "version";

    public static final String ENTITY_DETAILS_KEY = "entityCentricDetails";
    public static final String GEO_DETAILS_KEY = "geoDetails";
    public static final String PCJ_DETAILS_KEY = "pcjDetails";
    public static final String PCJ_ENABLED_KEY = "enabled";
    public static final String PCJ_FLUO_KEY = "fluoName";
    public static final String PCJ_PCJS_KEY = "pcjs";
    public static final String PCJ_ID_KEY = "id";
    public static final String PCJ_UPDATE_STRAT_KEY = "updateStrategy";
    public static final String PCJ_LAST_UPDATE_KEY = "lastUpdate";
    public static final String TEMPORAL_DETAILS_KEY = "temporalDetails";
    public static final String FREETEXT_DETAILS_KEY = "freeTextDetails";

    public static final String PROSPECTOR_DETAILS_KEY = "prospectorDetails";
    public static final String JOIN_SELECTIVITY_DETAILS_KEY = "joinSelectivitiyDetails";

    /**
     * Serializes {@link RyaDetails} to mongo {@link DBObject}.
     * @param details - The details to be serialized.
     * @return The mongo {@link DBObject}.
     */
    public static BasicDBObject toDBObject(final RyaDetails details) {
        Preconditions.checkNotNull(details);
        final BasicDBObjectBuilder builder = BasicDBObjectBuilder.start()
            .add(INSTANCE_KEY, details.getRyaInstanceName())
            .add(VERSION_KEY, details.getRyaVersion())
            .add(ENTITY_DETAILS_KEY, details.getEntityCentricIndexDetails().isEnabled())
            .add(GEO_DETAILS_KEY, details.getGeoIndexDetails().isEnabled())
            .add(PCJ_DETAILS_KEY, getPCJDetailsDBObject(details))
            .add(TEMPORAL_DETAILS_KEY, details.getTemporalIndexDetails().isEnabled())
            .add(FREETEXT_DETAILS_KEY, details.getFreeTextIndexDetails().isEnabled());
        if(details.getProspectorDetails().getLastUpdated().isPresent()) {
            builder.add(PROSPECTOR_DETAILS_KEY, details.getProspectorDetails().getLastUpdated().get());
        }
        if(details.getJoinSelectivityDetails().getLastUpdated().isPresent()) {
            builder.add(JOIN_SELECTIVITY_DETAILS_KEY, details.getJoinSelectivityDetails().getLastUpdated().get());
        }
        return (BasicDBObject) builder.get();
    }

    private static DBObject getPCJDetailsDBObject(final RyaDetails details) {
        final PCJIndexDetails pcjDetails = details.getPCJIndexDetails();
        final BasicDBObjectBuilder pcjBuilder = BasicDBObjectBuilder.start()
            .add(PCJ_ENABLED_KEY, pcjDetails.isEnabled());
        if(pcjDetails.getFluoDetails().isPresent()) {
            pcjBuilder.add(PCJ_FLUO_KEY, pcjDetails.getFluoDetails().get().getUpdateAppName());
        }
        final List<DBObject> pcjDetailList = new ArrayList<>();
        for(final Entry<String, PCJDetails> entry : pcjDetails.getPCJDetails().entrySet()) {
            final PCJDetails pcjDetail = entry.getValue();
            final BasicDBObjectBuilder indBuilbder = BasicDBObjectBuilder.start()
                .add(PCJ_ID_KEY, pcjDetail.getId())
                .add(PCJ_UPDATE_STRAT_KEY, pcjDetail.getUpdateStrategy().name());
            if(pcjDetail.getLastUpdateTime().isPresent()) {
                indBuilbder.add(PCJ_LAST_UPDATE_KEY, pcjDetail.getLastUpdateTime().get());
            }
            pcjDetailList.add(indBuilbder.get());
        }
        pcjBuilder.add(PCJ_PCJS_KEY, pcjDetailList.toArray());
        return pcjBuilder.get();
    }

    public static RyaDetails toRyaDetails(final DBObject mongoObj) throws MalformedRyaDetailsException {
        final BasicDBObject basicObj = (BasicDBObject) mongoObj;
        try {
        return RyaDetails.builder()
            .setRyaInstanceName(basicObj.getString(INSTANCE_KEY))
            .setRyaVersion(basicObj.getString(VERSION_KEY))
            .setEntityCentricIndexDetails(new EntityCentricIndexDetails(basicObj.getBoolean(ENTITY_DETAILS_KEY)))
            .setGeoIndexDetails(new GeoIndexDetails(basicObj.getBoolean(GEO_DETAILS_KEY)))
            .setPCJIndexDetails(getPCJDetails(basicObj))
            .setTemporalIndexDetails(new TemporalIndexDetails(basicObj.getBoolean(TEMPORAL_DETAILS_KEY)))
            .setFreeTextDetails(new FreeTextIndexDetails(basicObj.getBoolean(FREETEXT_DETAILS_KEY)))
            .setProspectorDetails(new ProspectorDetails(Optional.<Date>fromNullable(basicObj.getDate(PROSPECTOR_DETAILS_KEY))))
            .setJoinSelectivityDetails(new JoinSelectivityDetails(Optional.<Date>fromNullable(basicObj.getDate(JOIN_SELECTIVITY_DETAILS_KEY))))
            .build();
        } catch(final Exception e) {
            throw new MalformedRyaDetailsException("Failed to make RyaDetail from Mongo Object, it is malformed.", e);
        }
    }

    private static PCJIndexDetails getPCJDetails(final BasicDBObject basicObj) {
        final BasicDBObject pcjObj = (BasicDBObject) basicObj.get(PCJ_DETAILS_KEY);
        final PCJIndexDetails.Builder pcjBuilder = PCJIndexDetails.builder()
            .setEnabled(pcjObj.getBoolean(PCJ_ENABLED_KEY))
            .setFluoDetails(new FluoDetails(pcjObj.getString(PCJ_FLUO_KEY)));
        final BasicDBList pcjs = (BasicDBList) pcjObj.get(PCJ_PCJS_KEY);
        if(pcjs != null) {
            for(int ii = 0; ii < pcjs.size(); ii++) {
                final BasicDBObject pcj = (BasicDBObject) pcjs.get(ii);
                pcjBuilder.addPCJDetails(
                PCJDetails.builder()
                    .setId(pcj.getString(PCJ_ID_KEY))
                    .setUpdateStrategy(PCJUpdateStrategy.valueOf((String)pcj.get(PCJ_UPDATE_STRAT_KEY)))
                    .setLastUpdateTime(pcj.getDate(PCJ_LAST_UPDATE_KEY))
                    .build());
            }
        }
        return pcjBuilder.build();
    }

    /**
     * Exception thrown when a MongoDB {@link DBObject} is malformed when attemptin
     * to adapt it into a {@link RyaDetails}.
     */
    public static class MalformedRyaDetailsException extends Exception {
        private static final long serialVersionUID = 1L;

        /**
         * Creates a new {@link MalformedRyaDetailsException}
         * @param message - The message to be displayed by the exception.
         * @param e - The source cause of the exception.
         */
        public MalformedRyaDetailsException(final String message, final Throwable e) {
            super(message, e);
        }
    }
}
