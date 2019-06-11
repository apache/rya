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
import org.bson.Document;

import com.google.common.base.Optional;

import edu.umd.cs.findbugs.annotations.DefaultAnnotation;
import edu.umd.cs.findbugs.annotations.NonNull;

/**
 * Serializes configuration details for use in Mongo.
 * The {@link Document} will look like:
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
     * Converts a {@link RyaDetails} object into its MongoDB {@link Document} equivalent.
     *
     * @param details - The details to convert. (not null)
     * @return The MongoDB {@link Document} equivalent.
     */
    public static Document toDocument(final RyaDetails details) {
        requireNonNull(details);

        final Document doc = new Document()
                .append(INSTANCE_KEY, details.getRyaInstanceName())
                .append(VERSION_KEY, details.getRyaVersion())
                .append(ENTITY_DETAILS_KEY, details.getEntityCentricIndexDetails().isEnabled())
                //RYA-215            .append(GEO_DETAILS_KEY, details.getGeoIndexDetails().isEnabled())
                .append(PCJ_DETAILS_KEY, toDocument(details.getPCJIndexDetails()))
                .append(TEMPORAL_DETAILS_KEY, details.getTemporalIndexDetails().isEnabled())
                .append(FREETEXT_DETAILS_KEY, details.getFreeTextIndexDetails().isEnabled());

        if(details.getProspectorDetails().getLastUpdated().isPresent()) {
            doc.append(PROSPECTOR_DETAILS_KEY, details.getProspectorDetails().getLastUpdated().get());
        }

        if(details.getJoinSelectivityDetails().getLastUpdated().isPresent()) {
            doc.append(JOIN_SELECTIVITY_DETAILS_KEY, details.getJoinSelectivityDetails().getLastUpdated().get());
        }

        // If the Rya Streams Details are present, then add them.
        if(details.getRyaStreamsDetails().isPresent()) {
            final RyaStreamsDetails ryaStreamsDetails = details.getRyaStreamsDetails().get();

            // The embedded object that holds onto the fields.
            final Document ryaStreamsFields = new Document()
                    .append(RYA_STREAMS_HOSTNAME_KEY, ryaStreamsDetails.getHostname())
                    .append(RYA_STREAMS_PORT_KEY, ryaStreamsDetails.getPort());

            // Add them to the main builder.
            doc.append(RYA_STREAMS_DETAILS_KEY, ryaStreamsFields);
        }

        return doc;
    }

    private static Document toDocument(final PCJIndexDetails pcjIndexDetails) {
        requireNonNull(pcjIndexDetails);

        final Document doc = new Document();

        // Is Enabled
        doc.append(PCJ_ENABLED_KEY, pcjIndexDetails.isEnabled());

        // Add the PCJDetail objects.
        final List<Document> pcjDetailsList = new ArrayList<>();
        for(final PCJDetails pcjDetails : pcjIndexDetails.getPCJDetails().values()) {
            pcjDetailsList.add( toDocument( pcjDetails ) );
        }
        doc.append(PCJ_PCJS_KEY, pcjDetailsList);

        return doc;
    }

    static Document toDocument(final PCJDetails pcjDetails) {
        requireNonNull(pcjDetails);

        final Document doc = new Document();

        // PCJ ID
        doc.append(PCJ_ID_KEY, pcjDetails.getId());

        // PCJ Update Strategy if present.
        if(pcjDetails.getUpdateStrategy().isPresent()) {
            doc.append(PCJ_UPDATE_STRAT_KEY, pcjDetails.getUpdateStrategy().get().name());
        }

        // Last Update Time if present.
        if(pcjDetails.getLastUpdateTime().isPresent()) {
            doc.append(PCJ_LAST_UPDATE_KEY, pcjDetails.getLastUpdateTime().get());
        }

        return doc;
    }

    /**
     * Converts a MongoDB {@link Document} into its {@link RyaDetails} equivalent.
     *
     * @param doc - The MongoDB document to convert. (not null)
     * @return The equivalent {@link RyaDetails} object.
     * @throws MalformedRyaDetailsException The MongoDB object could not be converted.
     */
    public static RyaDetails toRyaDetails(final Document doc) throws MalformedRyaDetailsException {
        requireNonNull(doc);
        try {
            final RyaDetails.Builder builder = RyaDetails.builder()
                .setRyaInstanceName(doc.getString(INSTANCE_KEY))
                .setRyaVersion(doc.getString(VERSION_KEY))
                .setEntityCentricIndexDetails(new EntityCentricIndexDetails(doc.getBoolean(ENTITY_DETAILS_KEY)))
                //RYA-215            .setGeoIndexDetails(new GeoIndexDetails(doc.getBoolean(GEO_DETAILS_KEY)))
                .setPCJIndexDetails(getPCJIndexDetails(doc))
                .setTemporalIndexDetails(new TemporalIndexDetails(doc.getBoolean(TEMPORAL_DETAILS_KEY)))
                .setFreeTextDetails(new FreeTextIndexDetails(doc.getBoolean(FREETEXT_DETAILS_KEY)))
                .setProspectorDetails(new ProspectorDetails(Optional.<Date>fromNullable(doc.getDate(PROSPECTOR_DETAILS_KEY))))
                .setJoinSelectivityDetails(new JoinSelectivityDetails(Optional.<Date>fromNullable(doc.getDate(JOIN_SELECTIVITY_DETAILS_KEY))));

            // If the Rya Streams Details are present, then add them.
            if(doc.containsKey(RYA_STREAMS_DETAILS_KEY)) {
                final Document streamsObject = doc.get(RYA_STREAMS_DETAILS_KEY, Document.class);
                final String hostname = streamsObject.getString(RYA_STREAMS_HOSTNAME_KEY);
                final int port = streamsObject.getInteger(RYA_STREAMS_PORT_KEY);
                builder.setRyaStreamsDetails(new RyaStreamsDetails(hostname, port));
            }

            return builder.build();

        } catch(final Exception e) {
            throw new MalformedRyaDetailsException("Failed to make RyaDetail from Mongo Object, it is malformed.", e);
        }
    }

    private static PCJIndexDetails.Builder getPCJIndexDetails(final Document document) {
        final Document pcjIndexDoc = document.get(PCJ_DETAILS_KEY, Document.class);

        final PCJIndexDetails.Builder pcjBuilder = PCJIndexDetails.builder();
        if (!pcjIndexDoc.getBoolean(PCJ_ENABLED_KEY)) {
            pcjBuilder.setEnabled(false);
        } else {
            pcjBuilder.setEnabled(true);//no fluo details to set since mongo has no fluo support
            final List<Document> pcjs = pcjIndexDoc.getList(PCJ_PCJS_KEY, Document.class);
            if (pcjs != null) {
                for (final Document pcj : pcjs) {
                    pcjBuilder.addPCJDetails(toPCJDetails(pcj));
                }
            }
        }
        return pcjBuilder;
    }

    static PCJDetails.Builder toPCJDetails(final Document doc) {
        requireNonNull(doc);

        // PCJ ID.
        final PCJDetails.Builder builder = PCJDetails.builder()
                .setId( doc.getString(PCJ_ID_KEY) );

        // PCJ Update Strategy if present.
        if(doc.containsKey(PCJ_UPDATE_STRAT_KEY)) {
            builder.setUpdateStrategy( PCJUpdateStrategy.valueOf( doc.getString(PCJ_UPDATE_STRAT_KEY) ) );
        }

        // Last Update Time if present.
        if(doc.containsKey(PCJ_LAST_UPDATE_KEY)) {
            builder.setLastUpdateTime( doc.getDate(PCJ_LAST_UPDATE_KEY) );
        }

        return builder;
    }

    /**
     * Indicates a MongoDB {@link Document} was malformed when attempting
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