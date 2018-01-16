/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.rya.mongodb.instance;

import static java.util.Objects.requireNonNull;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import org.apache.rya.api.instance.RyaDetails;
import org.apache.rya.api.instance.RyaDetails.EntityCentricIndexDetails;
import org.apache.rya.api.instance.RyaDetails.FreeTextIndexDetails;
import org.apache.rya.api.instance.RyaDetails.JoinSelectivityDetails;
import org.apache.rya.api.instance.RyaDetails.PCJIndexDetails;
import org.apache.rya.api.instance.RyaDetails.PCJIndexDetails.PCJDetails;
import org.apache.rya.api.instance.RyaDetails.PCJIndexDetails.PCJDetails.PCJUpdateStrategy;
import org.apache.rya.api.instance.RyaDetails.ProspectorDetails;
import org.apache.rya.api.instance.RyaDetails.RyaStreamsDetails;
import org.apache.rya.api.instance.RyaDetails.TemporalIndexDetails;

import com.google.common.base.Optional;
import com.mongodb.BasicDBList;
import com.mongodb.BasicDBObject;
import com.mongodb.BasicDBObjectBuilder;
import com.mongodb.DBObject;

import edu.umd.cs.findbugs.annotations.DefaultAnnotation;
import edu.umd.cs.findbugs.annotations.NonNull;

/**
 * Serializes configuration details for use in Mongo.
 * The {@link DBObject} will look like:
 * <pre>
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
 *   "ryaStreamsDetails": {
 *       "hostname": &lt;string&gt;
 *       "port": &lt;int&gt;
 *   }
 * }
 * </pre>
 */
@DefaultAnnotation(NonNull.class)
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

    public static final String RYA_STREAMS_DETAILS_KEY = "ryaStreamsDetails";
    public static final String RYA_STREAMS_HOSTNAME_KEY = "hostname";
    public static final String RYA_STREAMS_PORT_KEY = "port";

    /**
     * Converts a {@link RyaDetails} object into its MongoDB {@link DBObject} equivalent.
     *
     * @param details - The details to convert. (not null)
     * @return The MongoDB {@link DBObject} equivalent.
     */
    public static BasicDBObject toDBObject(final RyaDetails details) {
        requireNonNull(details);

        final BasicDBObjectBuilder builder = BasicDBObjectBuilder.start()
                .add(INSTANCE_KEY, details.getRyaInstanceName())
                .add(VERSION_KEY, details.getRyaVersion())
                .add(ENTITY_DETAILS_KEY, details.getEntityCentricIndexDetails().isEnabled())
                //RYA-215            .add(GEO_DETAILS_KEY, details.getGeoIndexDetails().isEnabled())
                .add(PCJ_DETAILS_KEY, toDBObject(details.getPCJIndexDetails()))
                .add(TEMPORAL_DETAILS_KEY, details.getTemporalIndexDetails().isEnabled())
                .add(FREETEXT_DETAILS_KEY, details.getFreeTextIndexDetails().isEnabled());

        if(details.getProspectorDetails().getLastUpdated().isPresent()) {
            builder.add(PROSPECTOR_DETAILS_KEY, details.getProspectorDetails().getLastUpdated().get());
        }

        if(details.getJoinSelectivityDetails().getLastUpdated().isPresent()) {
            builder.add(JOIN_SELECTIVITY_DETAILS_KEY, details.getJoinSelectivityDetails().getLastUpdated().get());
        }

        // If the Rya Streams Details are present, then add them.
        if(details.getRyaStreamsDetails().isPresent()) {
            final RyaStreamsDetails ryaStreamsDetails = details.getRyaStreamsDetails().get();

            // The embedded object that holds onto the fields.
            final DBObject ryaStreamsFields = BasicDBObjectBuilder.start()
                    .add(RYA_STREAMS_HOSTNAME_KEY, ryaStreamsDetails.getHostname())
                    .add(RYA_STREAMS_PORT_KEY, ryaStreamsDetails.getPort())
                    .get();

            // Add them to the main builder.
            builder.add(RYA_STREAMS_DETAILS_KEY, ryaStreamsFields);
        }

        return (BasicDBObject) builder.get();
    }

    private static DBObject toDBObject(final PCJIndexDetails pcjIndexDetails) {
        requireNonNull(pcjIndexDetails);

        final BasicDBObjectBuilder builder = BasicDBObjectBuilder.start();

        // Is Enabled
        builder.add(PCJ_ENABLED_KEY, pcjIndexDetails.isEnabled());

        // Add the PCJDetail objects.
        final List<DBObject> pcjDetailsList = new ArrayList<>();
        for(final PCJDetails pcjDetails : pcjIndexDetails.getPCJDetails().values()) {
            pcjDetailsList.add( toDBObject( pcjDetails ) );
        }
        builder.add(PCJ_PCJS_KEY, pcjDetailsList.toArray());

        return builder.get();
    }

    static DBObject toDBObject(final PCJDetails pcjDetails) {
        requireNonNull(pcjDetails);

        final BasicDBObjectBuilder builder = BasicDBObjectBuilder.start();

        // PCJ ID
        builder.add(PCJ_ID_KEY, pcjDetails.getId());

        // PCJ Update Strategy if present.
        if(pcjDetails.getUpdateStrategy().isPresent()) {
            builder.add(PCJ_UPDATE_STRAT_KEY, pcjDetails.getUpdateStrategy().get().name());
        }

        // Last Update Time if present.
        if(pcjDetails.getLastUpdateTime().isPresent()) {
            builder.add(PCJ_LAST_UPDATE_KEY, pcjDetails.getLastUpdateTime().get());
        }

        return builder.get();
    }

    /**
     * Converts a MongoDB {@link DBObject} into its {@link RyaDetails} equivalent.
     *
     * @param mongoObj - The MongoDB object to convert. (not null)
     * @return The equivalent {@link RyaDetails} object.
     * @throws MalformedRyaDetailsException The MongoDB object could not be converted.
     */
    public static RyaDetails toRyaDetails(final DBObject mongoObj) throws MalformedRyaDetailsException {
        requireNonNull(mongoObj);
        final BasicDBObject basicObj = (BasicDBObject) mongoObj;
        try {
            final RyaDetails.Builder builder = RyaDetails.builder()
                .setRyaInstanceName(basicObj.getString(INSTANCE_KEY))
                .setRyaVersion(basicObj.getString(VERSION_KEY))
                .setEntityCentricIndexDetails(new EntityCentricIndexDetails(basicObj.getBoolean(ENTITY_DETAILS_KEY)))
                //RYA-215            .setGeoIndexDetails(new GeoIndexDetails(basicObj.getBoolean(GEO_DETAILS_KEY)))
                .setPCJIndexDetails(getPCJIndexDetails(basicObj))
                .setTemporalIndexDetails(new TemporalIndexDetails(basicObj.getBoolean(TEMPORAL_DETAILS_KEY)))
                .setFreeTextDetails(new FreeTextIndexDetails(basicObj.getBoolean(FREETEXT_DETAILS_KEY)))
                .setProspectorDetails(new ProspectorDetails(Optional.<Date>fromNullable(basicObj.getDate(PROSPECTOR_DETAILS_KEY))))
                .setJoinSelectivityDetails(new JoinSelectivityDetails(Optional.<Date>fromNullable(basicObj.getDate(JOIN_SELECTIVITY_DETAILS_KEY))));

            // If the Rya Streams Details are present, then add them.
            if(basicObj.containsField(RYA_STREAMS_DETAILS_KEY)) {
                final BasicDBObject streamsObject = (BasicDBObject) basicObj.get(RYA_STREAMS_DETAILS_KEY);
                final String hostname = streamsObject.getString(RYA_STREAMS_HOSTNAME_KEY);
                final int port = streamsObject.getInt(RYA_STREAMS_PORT_KEY);
                builder.setRyaStreamsDetails(new RyaStreamsDetails(hostname, port));
            }

            return builder.build();

        } catch(final Exception e) {
            throw new MalformedRyaDetailsException("Failed to make RyaDetail from Mongo Object, it is malformed.", e);
        }
    }

    private static PCJIndexDetails.Builder getPCJIndexDetails(final BasicDBObject basicObj) {
        final BasicDBObject pcjIndexDBO = (BasicDBObject) basicObj.get(PCJ_DETAILS_KEY);

        final PCJIndexDetails.Builder pcjBuilder = PCJIndexDetails.builder();
        if (!pcjIndexDBO.getBoolean(PCJ_ENABLED_KEY)) {
            pcjBuilder.setEnabled(false);
        } else {
            pcjBuilder.setEnabled(true);//no fluo details to set since mongo has no fluo support
            final BasicDBList pcjs = (BasicDBList) pcjIndexDBO.get(PCJ_PCJS_KEY);
            if (pcjs != null) {
                for (int ii = 0; ii < pcjs.size(); ii++) {
                    final BasicDBObject pcj = (BasicDBObject) pcjs.get(ii);
                    pcjBuilder.addPCJDetails(toPCJDetails(pcj));
                }
            }
        }
        return pcjBuilder;
    }

    static PCJDetails.Builder toPCJDetails(final BasicDBObject dbo) {
        requireNonNull(dbo);

        // PCJ ID.
        final PCJDetails.Builder builder = PCJDetails.builder()
                .setId( dbo.getString(PCJ_ID_KEY) );

        // PCJ Update Strategy if present.
        if(dbo.containsField(PCJ_UPDATE_STRAT_KEY)) {
            builder.setUpdateStrategy( PCJUpdateStrategy.valueOf( dbo.getString(PCJ_UPDATE_STRAT_KEY) ) );
        }

        // Last Update Time if present.
        if(dbo.containsField(PCJ_LAST_UPDATE_KEY)) {
            builder.setLastUpdateTime( dbo.getDate(PCJ_LAST_UPDATE_KEY) );
        }

        return builder;
    }

    /**
     * Indicates a MongoDB {@link DBObject} was malformed when attempting
     * to convert it into a {@link RyaDetails} object.
     */
    public static class MalformedRyaDetailsException extends Exception {
        private static final long serialVersionUID = 1L;

        /**
         * Creates a new {@link MalformedRyaDetailsException}.
         *
         * @param message - The message to be displayed by the exception.
         * @param e - The source cause of the exception.
         */
        public MalformedRyaDetailsException(final String message, final Throwable e) {
            super(message, e);
        }
    }
}