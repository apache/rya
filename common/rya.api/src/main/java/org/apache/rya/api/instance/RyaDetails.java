package org.apache.rya.api.instance;

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

import static java.util.Objects.requireNonNull;

import java.io.Serializable;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;

import javax.annotation.Nullable;
import javax.annotation.ParametersAreNonnullByDefault;
import javax.annotation.concurrent.Immutable;

import com.google.common.base.Optional;
import com.google.common.collect.ImmutableMap;

/**
 * Details about how a Rya instance's state.
 */
@Immutable
@ParametersAreNonnullByDefault
public class RyaDetails implements Serializable {
    private static final long serialVersionUID = 1L;

    // General metadata about the instance.
    private final String instanceName;
    private final String version;

    // Secondary Index Details.
    private final EntityCentricIndexDetails entityCentricDetails;
    private final GeoIndexDetails geoDetails;
    private final PCJIndexDetails pcjDetails;
    private final TemporalIndexDetails temporalDetails;
    private final FreeTextIndexDetails freeTextDetails;

    // Statistics Details.
    private final ProspectorDetails prospectorDetails;
    private final JoinSelectivityDetails joinSelectivityDetails;

    /**
     * Private to prevent initialization through the constructor. To build
     * instances of this class, use the {@link Builder}.
     */
    private RyaDetails(
            final String instanceName,
            final String version,
            final EntityCentricIndexDetails entityCentricDetails,
            final GeoIndexDetails geoDetails,
            final PCJIndexDetails pcjDetails,
            final TemporalIndexDetails temporalDetails,
            final FreeTextIndexDetails freeTextDetails,
            final ProspectorDetails prospectorDetails,
            final JoinSelectivityDetails joinSelectivityDetails) {
        this.instanceName = requireNonNull(instanceName);
        this.version = requireNonNull(version);
        this.entityCentricDetails = requireNonNull(entityCentricDetails);
        this.geoDetails = requireNonNull(geoDetails);
        this.pcjDetails = requireNonNull(pcjDetails);
        this.temporalDetails = requireNonNull(temporalDetails);
        this.freeTextDetails = requireNonNull(freeTextDetails);
        this.prospectorDetails = requireNonNull(prospectorDetails);
        this.joinSelectivityDetails = requireNonNull(joinSelectivityDetails);
    }

    /**
     * @return The name that uniquely identifies the instance of Rya within
     *   the system that hosts it.
     */
    public String getRyaInstanceName() {
        return instanceName;
    }

    /**
     * @return The version of Rya this instance uses.
     */
    public String getRyaVersion() {
        return version;
    }

    /**
     * @return Information about the instance's Entity Centric Index.
     */
    public EntityCentricIndexDetails getEntityCentricIndexDetails() {
        return entityCentricDetails;
    }

    /**
     * @return Information about the instance's Geospatial Index.
     */
    public GeoIndexDetails getGeoIndexDetails() {
        return geoDetails;
    }

    /**
     * @return Information about the instance's Precomputed Join Index.
     */
    public PCJIndexDetails getPCJIndexDetails() {
        return pcjDetails;
    }

    /**
     * @return Information about the instance's Temporal Index.
     */
    public TemporalIndexDetails getTemporalIndexDetails() {
        return temporalDetails;
    }

    /**
     * @return Information about the instance's Free Text Index.
     */
    public FreeTextIndexDetails getFreeTextIndexDetails() {
        return freeTextDetails;
    }

    /**
     * @return Information about the instance's Prospector Statistics.
     */
    public ProspectorDetails getProspectorDetails() {
        return prospectorDetails;
    }

    /**
     * @return Information about the instance's Join Selectivity Statistics.
     */
    public JoinSelectivityDetails getJoinSelectivityDetails() {
        return joinSelectivityDetails;
    }

    @Override
    public int hashCode() {
        return Objects.hash(
                instanceName,
                version,
                entityCentricDetails,
                geoDetails,
                pcjDetails,
                temporalDetails,
                freeTextDetails,
                prospectorDetails,
                joinSelectivityDetails);
    }

    @Override
    public boolean equals(final Object obj) {
        if(this == obj) {
            return true;
        }
        if(obj instanceof RyaDetails) {
            final RyaDetails details = (RyaDetails) obj;
            return Objects.equals(instanceName, details.instanceName) &&
                    Objects.equals(version, details.version) &&
                    Objects.equals(entityCentricDetails, details.entityCentricDetails) &&
                    Objects.equals(geoDetails, details.geoDetails) &&
                    Objects.equals(pcjDetails, details.pcjDetails) &&
                    Objects.equals(temporalDetails, details.temporalDetails) &&
                    Objects.equals(freeTextDetails, details.freeTextDetails) &&
                    Objects.equals(prospectorDetails, details.prospectorDetails) &&
                    Objects.equals(joinSelectivityDetails, details.joinSelectivityDetails);
        }
        return false;
    }

    /**
     * @return An empty instance of {@link Builder}.
     */
    public static Builder builder() {
        return new Builder();
    }

    /**
     * @param detials - The builder will be initialized with this object's values. (not null)
     * @return An instance of {@link Builder} that is initialized with a {@link RyaDetails}'s values.
     */
    public static Builder builder(final RyaDetails details) {
        return new Builder(details);
    }

    /**
     * Builds instances of {@link RyaDetails}.
     */
    @ParametersAreNonnullByDefault
    public static class Builder {

        // General metadata about the instance.
        private String instanceName;
        private String version;

        // Secondary Index Details.
        private EntityCentricIndexDetails entityCentricDetails;
        private GeoIndexDetails geoDetails;
        private PCJIndexDetails.Builder pcjIndexDetailsBuilder;
        private TemporalIndexDetails temporalDetails;
        private FreeTextIndexDetails freeTextDetails;

        // Statistics Details.
        private ProspectorDetails prospectorDetails;
        private JoinSelectivityDetails joinSelectivityDetails;

        /**
         * Construcst an empty instance of {@link Builder}.
         */
        public Builder() { }

        /**
         * Constructs an instance of {@link Builder} that is initialized with
         * a {@link RyaDetails}'s values.
         *
         * @param detials - The builder will be initialized with this object's values. (not null)
         */
        public Builder(final RyaDetails details) {
            requireNonNull(details);
            instanceName = details.instanceName;
            version = details.version;
            entityCentricDetails = details.entityCentricDetails;
            geoDetails = details.geoDetails;
            pcjIndexDetailsBuilder = PCJIndexDetails.builder( details.pcjDetails );
            temporalDetails = details.temporalDetails;
            freeTextDetails = details.freeTextDetails;
            prospectorDetails = details.prospectorDetails;
            joinSelectivityDetails = details.joinSelectivityDetails;
        }

        /**
         * @param instanceName - The name that uniquely identifies the instance of Rya within
         *   the system that hosts it.
         * @return This {@link Builder} so that method invocations may be chained.
         */
        public Builder setRyaInstanceName(@Nullable final String instanceName) {
            this.instanceName = instanceName;
            return this;
        }

        /**
         * @param version - The version of Rya this instance uses.
         * @return This {@link Builder} so that method invocations may be chained.
         */
        public Builder setRyaVersion(@Nullable final String version) {
            this.version = version;
            return this;
        }

        /**
         * @param entityCentricDetails - Information about the instance's Entity Centric Index.
         * @return This {@link Builder} so that method invocations may be chained.
         */
        public Builder setEntityCentricIndexDetails(@Nullable final EntityCentricIndexDetails entityCentricDetails) {
            this.entityCentricDetails = entityCentricDetails;
            return this;
        }

        /**
         *
         * @param geoDetails - Information about the instance's Geospatial Index.
         * @return This {@link Builder} so that method invocations may be chained.
         */
        public Builder setGeoIndexDetails(@Nullable final GeoIndexDetails geoDetails) {
            this.geoDetails = geoDetails;
            return this;
        }

        /**
         * @param temporalDetails - Information about the instance's Temporal Index.
         * @return This {@link Builder} so that method invocations may be chained.
         */
        public Builder setTemporalIndexDetails(@Nullable final TemporalIndexDetails temporalDetails) {
            this.temporalDetails = temporalDetails;
            return this;
        }

        /**
         * @param freeTextDetails - Information about the instance's Free Text Index.
         * @return This {@link Builder} so that method invocations may be chained.
         */
        public Builder setFreeTextDetails(@Nullable final FreeTextIndexDetails freeTextDetails) {
            this.freeTextDetails = freeTextDetails;
            return this;
        }

        /**
         * @param pcjDetails - Information about the instance's Precomputed Join Index.
         * @return This {@link Builder} so that method invocations may be chained.
         */
        public Builder setPCJIndexDetails(@Nullable final PCJIndexDetails.Builder pcjDetailsBuilder) {
            this.pcjIndexDetailsBuilder = pcjDetailsBuilder;
            return this;
        }

        /**
         * @return Get the {@link PCJIndexDetails.Builder} used to build the
         *   PCJ Index's details.
         */
        public @Nullable PCJIndexDetails.Builder getPCJIndexDetails() {
            return pcjIndexDetailsBuilder;
        }

        /**
         * @param prospectorDetails - Information about the instance's Prospector Statistics.
         * @return This {@link Builder} so that method invocations may be chained.
         */
        public Builder setProspectorDetails(@Nullable final ProspectorDetails prospectorDetails) {
            this.prospectorDetails = prospectorDetails;
            return this;
        }

        /**
         * @param joinSelectivityDetails - Information about the instance's Join Selectivity Statistics.
         * @return This {@link Builder} so that method invocations may be chained.
         */
        public Builder setJoinSelectivityDetails(@Nullable final JoinSelectivityDetails joinSelectivityDetails) {
            this.joinSelectivityDetails = joinSelectivityDetails;
            return this;
        }

        /**
         * @return An instance of {@link RyaDetails} built using this
         *   builder's values.
         */
        public RyaDetails build() {
            return new RyaDetails(
                    instanceName,
                    version,
                    entityCentricDetails,
                    geoDetails,
                    pcjIndexDetailsBuilder.build(),
                    temporalDetails,
                    freeTextDetails,
                    prospectorDetails,
                    joinSelectivityDetails);
        }
    }

    /**
     * Details about a Rya instance's Geospatial Index.
     */
    @Immutable
    @ParametersAreNonnullByDefault
    public static class GeoIndexDetails implements Serializable {
        private static final long serialVersionUID = 1L;

        private final boolean enabled;

        /**
         * Constructs an instance of {@link GeoIndexDetails}.
         *
         * @param enabled - Whether or not a Geospatial Index will be maintained by the Rya instance.
         */
        public GeoIndexDetails(final boolean enabled) {
            this.enabled = enabled;
        }

        /**
         * @return Whether or not a Geospatial Index will be maintained by the Rya instance.
         */
        public boolean isEnabled() {
            return enabled;
        }

        @Override
        public int hashCode() {
            return Objects.hash( enabled );
        }

        @Override
        public boolean equals(final Object obj) {
            if(this == obj) {
                return true;
            }
            if(obj instanceof GeoIndexDetails) {
                final GeoIndexDetails details = (GeoIndexDetails) obj;
                return enabled == details.enabled;
            }
            return false;
        }
    }

    /**
     * Details about a Rya instance's Temporal Index.
     */
    @Immutable
    @ParametersAreNonnullByDefault
    public static class TemporalIndexDetails implements Serializable {
        private static final long serialVersionUID = 1L;

        private final boolean enabled;

        /**
         * Constructs an instance of {@link TemporalIndexDetails}.
         *
         * @param enabled - Whether or not a Temporal Index will be maintained by the Rya instance.
         */
        public TemporalIndexDetails(final boolean enabled) {
            this.enabled = enabled;
        }

        /**
         * @return Whether or not a Temporal Index will be maintained by the Rya instance.
         */
        public boolean isEnabled() {
            return enabled;
        }

        @Override
        public int hashCode() {
            return Objects.hash( enabled );
        }

        @Override
        public boolean equals(final Object obj) {
            if(this == obj) {
                return true;
            }
            if(obj instanceof TemporalIndexDetails) {
                final TemporalIndexDetails details = (TemporalIndexDetails) obj;
                return enabled == details.enabled;
            }
            return false;
        }
    }

    /**
     * Details about a Rya instance's Entity Centric Index.
     */
    @Immutable
    @ParametersAreNonnullByDefault
    public static class EntityCentricIndexDetails implements Serializable {
        private static final long serialVersionUID = 1L;

        private final boolean enabled;

        /**
         * Constructs an instance of {@link EntityCentricIndexDetails}.
         *
         * @param enabled - Whether or not a Entity Centric Index will be maintained by the Rya instance.
         */
        public EntityCentricIndexDetails(final boolean enabled) {
            this.enabled = enabled;
        }

        /**
         * @return Whether or not a Entity Centric Index will be maintained by the Rya instance.
         */
        public boolean isEnabled() {
            return enabled;
        }

        @Override
        public int hashCode() {
            return Objects.hash( enabled );
        }

        @Override
        public boolean equals(final Object obj) {
            if(this == obj) {
                return true;
            }
            if(obj instanceof EntityCentricIndexDetails) {
                final EntityCentricIndexDetails details = (EntityCentricIndexDetails) obj;
                return enabled == details.enabled;
            }
            return false;
        }
    }

    /**
     * Details about a Rya instance's Free Text Index.
     */
    @Immutable
    @ParametersAreNonnullByDefault
    public static class FreeTextIndexDetails implements Serializable {
        private static final long serialVersionUID = 1L;

        private final boolean enabled;

        /**
         * Constructs an instance of {@link FreeTextIndexDetails}.
         *
         * @param enabled - Whether or not a Free Text Index will be maintained by the Rya instance.
         */
        public FreeTextIndexDetails(final boolean enabled) {
            this.enabled = enabled;
        }

        /**
         * @return Whether or not a Free Text Index will be maintained by the Rya instance.
         */
        public boolean isEnabled() {
            return enabled;
        }

        @Override
        public int hashCode() {
            return Objects.hash( enabled );
        }

        @Override
        public boolean equals(final Object obj) {
            if(this == obj) {
                return true;
            }
            if(obj instanceof FreeTextIndexDetails) {
                final FreeTextIndexDetails details = (FreeTextIndexDetails) obj;
                return enabled == details.enabled;
            }
            return false;
        }
    }

    /**
     * Details about a Rya instance's PCJ Index.
     */
    @Immutable
    @ParametersAreNonnullByDefault
    public static class PCJIndexDetails implements Serializable {
        private static final long serialVersionUID = 1L;

        public final boolean enabled;
        private final Optional<FluoDetails> fluoDetails;
        private final ImmutableMap<String, PCJDetails> pcjDetails;

        /**
         * Private to prevent initialization through the constructor. To build
         * instances of this class, use the {@link Builder}.
         *
         * @param enabled - Whether or not a Precomputed Join Index will be maintained by the Rya instance.
         * @param fluoDetails - Details about a Fluo application that is used to
         *   incrementally update PCJs if one has been installed for this RYA
         *   instance. (not null)
         * @param pcjDetails - Details about the PCJs that have been created
         *   for this Rya instance. (not null)
         */
        private PCJIndexDetails(
                final boolean enabled,
                final Optional<FluoDetails> fluoDetails,
                final ImmutableMap<String, PCJDetails> pcjDetails) {
            this.enabled = enabled;
            this.fluoDetails = requireNonNull(fluoDetails);
            this.pcjDetails = requireNonNull(pcjDetails);
        }

        /**
         * @return Whether or not a Precomputed Join Index will be maintained by the Rya instance.
         */
        public boolean isEnabled() {
            return enabled;
        }

        /**
         * @return Details about a Fluo application that is used to incrementally
         *   update PCJs if one has been installed for this RYA instance.
         */
        public Optional<FluoDetails> getFluoDetails() {
            return fluoDetails;
        }

        /**
         * @return Details about the PCJs that have been created for this Rya instance.
         *   The key is the PCJ ID and the value are the details for the ID.
         */
        public ImmutableMap<String, PCJDetails> getPCJDetails() {
            return pcjDetails;
        }

        @Override
        public int hashCode() {
            return Objects.hash(enabled, fluoDetails, pcjDetails);
        }

        @Override
        public boolean equals(final Object obj) {
            if(this == obj) {
                return true;
            }
            if(obj instanceof PCJIndexDetails) {
                final PCJIndexDetails details = (PCJIndexDetails) obj;
                return Objects.equals(enabled, details.enabled) &&
                        Objects.equals(fluoDetails,  details.fluoDetails) &&
                        Objects.equals(pcjDetails, details.pcjDetails);
            }
            return false;
        }

        /**
         * @return A new instance of {@link Builder}.
         */
        public static Builder builder() {
            return new Builder();
        }

        /**
         * @param detials - The builder will be initialized with this object's values. (not null)
         * @return An instance of {@link Builder} that is initialized with a {@link PCJIndexDetails}'s values.
         */
        public static Builder builder(final PCJIndexDetails pcjIndexDetails) {
            return new Builder(pcjIndexDetails);
        }

        /**
         * Builds instance of {@link PCJIndexDetails).
         */
        @ParametersAreNonnullByDefault
        public static class Builder {

            private Boolean enabled = null;
            private FluoDetails fluoDetails = null;
            private final Map<String, PCJDetails.Builder> pcjDetailsBuilders = new HashMap<>();

            /**
             * Constructs an empty instance of {@link Builder}.
             */
            public Builder() { }

            /**
             * Constructs an instance of {@link Builder} that is initialized with
             * the values of a {@link PCJIndexDetails}.
             *
             * @param pcjIndexDetails - This objects values will be used to initialize
             *   the builder. (not null)
             */
            public Builder(final PCJIndexDetails pcjIndexDetails) {
                requireNonNull(pcjIndexDetails);
                this.enabled = pcjIndexDetails.enabled;
                this.fluoDetails = pcjIndexDetails.fluoDetails.orNull();

                for(final PCJDetails pcjDetails : pcjIndexDetails.pcjDetails.values()) {
                    pcjDetailsBuilders.put(pcjDetails.getId(), PCJDetails.builder(pcjDetails));
                }
            }

            /**
             * @param enabled - Whether or not a Precomputed Join Index will be maintained by the Rya instance.
             * @return This {@link Builder} so that method invocations may be chained.
             */
            public Builder setEnabled(final Boolean enabled) {
                this.enabled = enabled;
                return this;
            }

            /**
             * @param fluoDetails - Details about a Fluo application that is used
             *   to incrementally update PCJs if one has been installed for this RYA instance.
             * @return This {@link Builder} so that method invocations may be chained.
             */
            public Builder setFluoDetails(@Nullable final FluoDetails fluoDetails) {
                this.fluoDetails = fluoDetails;
                return this;
            }

            /**
             * @param pcjDetails - Details about the PCJs that have been created for this Rya instance.
             * @return This {@link Builder} so that method invocations may be chained.
             */
            public Builder addPCJDetails(@Nullable final PCJDetails.Builder pcjDetailsBuilder) {
                if(pcjDetailsBuilder != null) {
                    this.pcjDetailsBuilders.put(pcjDetailsBuilder.getId(), pcjDetailsBuilder);
                }
                return this;
            }

            /**
             * @param pcjId - The PCJ ID of the {@link PCJDetails.Builder} to remove. (not null)
             * @return This {@link Builder} so that method invocations may be chained.
             */
            public Builder removePCJDetails(@Nullable final String pcjId) {
                requireNonNull(pcjId);
                this.pcjDetailsBuilders.remove(pcjId);
                return this;
            }

            /**
             * @return Builds an instance of {@link PCJIndexDetails} using this builder's values.
             */
            public PCJIndexDetails build() {
                final ImmutableMap.Builder<String, PCJDetails> pcjDetails = ImmutableMap.builder();
                for(final Entry<String, PCJDetails.Builder> entry : pcjDetailsBuilders.entrySet()) {
                    pcjDetails.put(entry.getKey(), entry.getValue().build());
                }

                return new PCJIndexDetails(
                        enabled,
                        Optional.fromNullable( fluoDetails ),
                        pcjDetails.build());
            }
        }

        /**
         * Details about a Fluo Incremental PCJ application that has been installed
         * as part of this Rya instance.
         */
        @Immutable
        @ParametersAreNonnullByDefault
        public static class FluoDetails implements Serializable {
            private static final long serialVersionUID = 1L;

            private final String updateAppName;

            /**
             * Constructs an instance of {@link FluoDetails}.
             *
             * @param updateAppName - The name of the Fluo application that is
             *   updating this Rya instance's incremental PCJs. (not null)
             */
            public FluoDetails(final String updateAppName) {
                this.updateAppName = requireNonNull(updateAppName);
            }

            /**
             * @return The name of the Fluo application.
             */
            public String getUpdateAppName() {
                return updateAppName;
            }

            @Override
            public int hashCode() {
                return Objects.hash(updateAppName);
            }

            @Override
            public boolean equals(final Object obj) {
                if(this == obj) {
                    return true;
                }
                if(obj instanceof FluoDetails) {
                    final FluoDetails details = (FluoDetails) obj;
                    return Objects.equals(updateAppName, details.updateAppName);
                }
                return false;
            }
        }

        /**
         * Details about a specific PCJ that is being maintained within the Rya instance.
         */
        @Immutable
        @ParametersAreNonnullByDefault
        public static class PCJDetails implements Serializable {
            private static final long serialVersionUID = 1L;

            private final String id;
            private final Optional<PCJUpdateStrategy> updateStrategy;
            private final Optional<Date> lastUpdateTime;

            /**
             * Private to prevent initialization through the constructor. To build
             * instances of this class, use the {@link Builder}.
             *
             * @param id - Uniquely identifies the PCJ within this instance of Rya. (not null)
             * @param updateStrategy - Describes how the PCJ is being updated. (not null)
             * @param lastUpdateTime - The last time the PCJ was updated. This information
             *   may not be provided. (not null)
             */
            private PCJDetails(
                    final String id,
                    final Optional<PCJUpdateStrategy> updateStrategy,
                    final Optional<Date> lastUpdateTime) {
                this.id = requireNonNull(id);
                this.updateStrategy = requireNonNull(updateStrategy);
                this.lastUpdateTime = requireNonNull(lastUpdateTime);
            }

            /**
             * @return Uniquely identifies the PCJ within this instance of Rya.
             */
            public String getId() {
                return id;
            }

            /**
             * @return Describes how the PCJ is being updated.
             */
            public Optional<PCJUpdateStrategy> getUpdateStrategy() {
                return updateStrategy;
            }

            /**
             * @return The last time the PCJ was updated. This information
             *   may not be provided.
             */
            public Optional<Date> getLastUpdateTime() {
                return lastUpdateTime;
            }

            @Override
            public int hashCode() {
                return Objects.hash(id, updateStrategy, lastUpdateTime);
            }

            @Override
            public boolean equals(final Object obj) {
                if(this == obj) {
                    return true;
                }
                if(obj instanceof PCJDetails) {
                    final PCJDetails details = (PCJDetails) obj;
                    return Objects.equals(id, details.id) &&
                            Objects.equals(updateStrategy, details.updateStrategy) &&
                            Objects.equals(lastUpdateTime, details.lastUpdateTime);
                }
                return false;
            }

            /**
             * @return A new instance of {@link Builder}.
             */
            public static Builder builder() {
                return new Builder();
            }

            /**
             * @param detials - The builder will be initialized with this object's values. (not null)
             * @return An instance of {@link Builder} that is initialized with a {@link PCJDetails}' values.
             */
            public static Builder builder(final PCJDetails details) {
                return new Builder(details);
            }

            /**
             * Builds instance of {@link PCJDetails}.
             */
            @ParametersAreNonnullByDefault
            public static class Builder {

                private String id;
                private PCJUpdateStrategy updateStrategy;
                private Date lastUpdateTime;

                /**
                 * Constructs an instance of {@link Builder}.
                 */
                public Builder() { }

                /**
                 * Constructs an instance of {@link Builder} that is initialized with
                 * the values of a {@link PCJDetails}.
                 *
                 * @param details - This object's values will be used to initialize the builder. (not null)
                 */
                public Builder(final PCJDetails details) {
                    requireNonNull(details);
                    this.id = details.id;
                    this.updateStrategy = details.updateStrategy.orNull();
                    this.lastUpdateTime = details.lastUpdateTime.orNull();
                }

                /**
                 * @return Uniquely identifies the PCJ within this instance of Rya.
                 */
                public @Nullable String getId() {
                    return id;
                }

                /**
                 * @param id - Uniquely identifies the PCJ within this instance of Rya.
                 * @return This {@link Builder} so that method invocations may be chained.
                 */
                public Builder setId(@Nullable final String id) {
                    this.id = id;
                    return this;
                }

                /**
                 * @return Describes how the PCJ is being updated.
                 */
                public PCJUpdateStrategy getUpdateStrategy() {
                    return updateStrategy;
                }

                /**
                 * @param updateStrategy - Describes how the PCJ is being updated.
                 * @return This {@link Builder} so that method invocations may be chained.
                 */
                public Builder setUpdateStrategy(@Nullable final PCJUpdateStrategy updateStrategy) {
                    this.updateStrategy = updateStrategy;
                    return this;
                }

                /**
                 * @return The last time the PCJ was updated. This information may not be provided.
                 */
                public @Nullable Date getLastUpdateTime() {
                    return lastUpdateTime;
                }

                /**
                 * @param lastUpdateTime - The last time the PCJ was updated. This information
                 *   may not be provided.
                 * @return This {@link Builder} so that method invocations may be chained.
                 */
                public Builder setLastUpdateTime(@Nullable final Date lastUpdateTime) {
                    this.lastUpdateTime = lastUpdateTime;
                    return this;
                }

                /**
                 * @return An instance of {@link PCJDetails} built using this builder's values.
                 */
                public PCJDetails build() {
                    return new PCJDetails(
                            id,
                            Optional.fromNullable(updateStrategy),
                            Optional.fromNullable(lastUpdateTime));
                }
            }

            /**
             * Describes the different strategies that may be used to update a PCJ index.
             */
            public static enum PCJUpdateStrategy {
                /**
                 * The PCJ is being updated by periodically rebuilding all of the results.
                 */
                BATCH,

                /**
                 * The PCJ is being updated frequently and incrementally as new
                 * Statements are inserted into the Tya instance.
                 */
                INCREMENTAL;
            }
        }
    }

    /**
     * Details about a Rya instance's Prospector statistics.
     */
    @Immutable
    @ParametersAreNonnullByDefault
    public static class ProspectorDetails implements Serializable {
        private static final long serialVersionUID = 1L;

        private final Optional<Date> lastUpdated;

        /**
         * Constructs an instance of {@link ProspectorDetails}.
         *
         * @param lastUpdated - The last time the Prospector statistics were updated for the Rya instance. (not null)
         */
        public ProspectorDetails(final Optional<Date> lastUpdated) {
            this.lastUpdated = requireNonNull(lastUpdated);
        }

        /**
         * @return The last time the Prospector statistics were updated for the Rya instance.
         */
        public Optional<Date> getLastUpdated() {
            return lastUpdated;
        }

        @Override
        public int hashCode() {
            return Objects.hash( lastUpdated );
        }

        @Override
        public boolean equals(final Object obj) {
            if(this == obj) {
                return true;
            }
            if(obj instanceof ProspectorDetails) {
                final ProspectorDetails details = (ProspectorDetails) obj;
                return Objects.equals(lastUpdated, details.lastUpdated);
            }
            return false;
        }
    }

    /**
     * Details about a Rya instance's Join Selectivity statistics.
     */
    @Immutable
    @ParametersAreNonnullByDefault
    public static class JoinSelectivityDetails implements Serializable {
        private static final long serialVersionUID = 1L;

        private final Optional<Date> lastUpdated;

        /**
         * Constructs an instance of {@link JoinSelectivityDetails}.
         *
         * @param lastUpdated - The last time the Join Selectivity statistics were updated for the Rya instance. (not null)
         */
        public JoinSelectivityDetails(final Optional<Date> lastUpdated) {
            this.lastUpdated = requireNonNull(lastUpdated);
        }

        /**
         * @return The last time the Join Selectivity statistics were updated for the Rya instance.
         */
        public Optional<Date> getLastUpdated() {
            return lastUpdated;
        }

        @Override
        public int hashCode() {
            return Objects.hash( lastUpdated );
        }

        @Override
        public boolean equals(final Object obj) {
            if(this == obj) {
                return true;
            }
            if(obj instanceof JoinSelectivityDetails) {
                final JoinSelectivityDetails details = (JoinSelectivityDetails) obj;
                return Objects.equals(lastUpdated, details.lastUpdated);
            }
            return false;
        }
    }
}