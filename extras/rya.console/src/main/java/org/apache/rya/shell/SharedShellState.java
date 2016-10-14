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
package org.apache.rya.shell;

import static java.util.Objects.requireNonNull;

import java.util.Objects;
import java.util.concurrent.locks.ReentrantLock;

import javax.annotation.Nullable;
import javax.annotation.ParametersAreNonnullByDefault;
import javax.annotation.concurrent.Immutable;
import javax.annotation.concurrent.ThreadSafe;

import com.google.common.base.Optional;

import org.apache.rya.api.client.RyaClient;
import org.apache.rya.api.client.accumulo.AccumuloConnectionDetails;

/**
 * Holds values that are shared between the various Rya command classes.
 */
@ThreadSafe
@ParametersAreNonnullByDefault
public class SharedShellState {
    // The shared nature of this object means we shouldn't assume only a single thread is accessing it.
    private final ReentrantLock lock = new ReentrantLock();

    // The current state.
    private ShellState shellState = ShellState.builder()
            .setConnectionState( ConnectionState.DISCONNECTED )
            .build();

    /**
     * @return The values that define the state of the Rya Shell.
     */
    public ShellState getShellState() {
        lock.lock();
        try {
            return shellState;
        } finally {
            lock.unlock();
        }
    }

    /**
     * This method indicates a shift into the {@link ConnectionState#CONNECTED_TO_STORAGE} state.
     * <p/>
     * Store the values used by an Accumulo Rya Storage connection. This may
     * only be called when the shell is disconnected.
     *
     * @param connectionDetails - Metadata about the Accumulo connection. (not null)
     * @param connectedCommands - Rya Commands that will execute against the Accumulo instance. (not null)
     * @throws IllegalStateException Thrown if the shell is already connected to a Rya storage.
     */
    public void connectedToAccumulo(
            final AccumuloConnectionDetails connectionDetails,
            final RyaClient connectedCommands) throws IllegalStateException {
        requireNonNull(connectionDetails);
        requireNonNull(connectedCommands);

        lock.lock();
        try {
            // Ensure the Rya Shell is disconnected.
            if(shellState.getConnectionState() != ConnectionState.DISCONNECTED) {
                throw new IllegalStateException("You must clear the old connection state before you may set a new connection state.");
            }

            // Store the connection details.
            shellState = ShellState.builder()
                .setConnectionState( ConnectionState.CONNECTED_TO_STORAGE )
                .setAccumuloConnectionDetails( connectionDetails )
                .setConnectedCommands( connectedCommands )
                .build();
        } finally {
            lock.unlock();
        }
    }

    /**
     * This method indicates a shift into the {@link ConnectionState#CONNECTED_TO_INSTANCE} state.
     * <p/>
     * Store the name of the Rya instance all commands will be executed against.
     *
     * @param instanceName - The name of the Rya instance. (not null)
     * @throws IllegalStateException Thrown if the shell is disconnected.
     */
    public void connectedToInstance(final String instanceName) throws IllegalStateException {
        requireNonNull(instanceName);

        lock.lock();
        try {
            // Verify the Rya Shell is connected to a storage.
            if(shellState.getConnectionState() == ConnectionState.DISCONNECTED) {
                throw new IllegalStateException("You can not set a Rya Instance Name before connecting to a Rya Storage.");
            }

            // Set the instance name.
            shellState = ShellState.builder( shellState )
                    .setConnectionState(ConnectionState.CONNECTED_TO_INSTANCE)
                    .setRyaInstanceName( instanceName )
                    .build();
        } finally {
            lock.unlock();
        }
    }

    /**
     * This method indicates a shift into the {@link DISCONNECTED} state.
     * <p/>
     * Clears all of the values associated with a Rya Storage/Instance connection.
     * If the shell is already disconnected, then this method does not do anything.
     */
    public void disconnected() {
        lock.lock();
        try {
            shellState = ShellState.builder()
                .setConnectionState(ConnectionState.DISCONNECTED)
                .build();
        } finally {
            lock.unlock();
        }
    }

    /**
     * Enumerates the various states a Rya Shell may be in.
     */
    public static enum ConnectionState {
        /**
         * The shell is not connected to a Rya Storage.
         */
        DISCONNECTED,

        /**
         * The shell is connected to a Rya Storage, but a specific instance hasn't been set.
         */
        CONNECTED_TO_STORAGE,

        /**
         * The shell is connected to Rya Storage and a specific Rya Instance.
         */
        CONNECTED_TO_INSTANCE;
    }

    /**
     * Values that define the state of a Rya Shell.
     */
    @Immutable
    @ParametersAreNonnullByDefault
    public static final class ShellState {
        // Indicates the state of the shell.
        private final ConnectionState connectionState;

        // Connection specific values.
        private final Optional<AccumuloConnectionDetails> connectionDetails;
        private final Optional<RyaClient> connectedCommands;

        // Instance specific values.
        private final Optional<String> instanceName;

        private ShellState(
                final ConnectionState connectionState,
                final Optional<AccumuloConnectionDetails> connectionDetails,
                final Optional<RyaClient> connectedCommands,
                final Optional<String> instanceName) {
            this.connectionState = requireNonNull(connectionState);
            this.connectionDetails = requireNonNull(connectionDetails);
            this.connectedCommands = requireNonNull(connectedCommands);
            this.instanceName = requireNonNull(instanceName);
        }

        /**
         * @return The {@link ConnectionState} of the Rya Shell.
         */
        public ConnectionState getConnectionState() {
            return connectionState;
        }

        /**
         * @return Metadata about the Accumulo connection. The value will not be present
         *   if the Rya Shell is not connected to a storage.
         */
        public Optional<AccumuloConnectionDetails> getConnectionDetails() {
            return connectionDetails;
        }

        /**
         * @return The {@link RyaClient} to use when a command on the shell is issued.
         *   The value will not be present if the Rya Shell is not connected to a storage.
         */
        public Optional<RyaClient> getConnectedCommands() {
            return connectedCommands;
        }

        /**
         * @return The name of the Rya Instance the Rya Shell is issuing commands to.
         *   The value will not be present if the Rya Shell is not connected to a
         *   storage or if a target instance has not been set yet.
         */
        public Optional<String> getRyaInstanceName() {
            return instanceName;
        }

        @Override
        public int hashCode() {
            return Objects.hash(connectionState, connectionDetails, connectedCommands, instanceName);
        }

        @Override
        public boolean equals(final Object obj) {
            if(this == obj) {
                return true;
            }
            if(obj instanceof ShellState) {
                final ShellState state = (ShellState)obj;
                return Objects.equals(connectionState, state.connectionState) &&
                        Objects.equals(connectionDetails, state.connectionDetails) &&
                        Objects.equals(connectedCommands, state.connectedCommands) &&
                        Objects.equals(instanceName, state.instanceName);
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
         * Create an instance of {@link Builder} populated with the values of {code shellState}.
         *
         * @param shellState - The initial state of the Builder.
         * @return An instance of {@link Builder} populated with the values
         *   of {code shellState}.
         */
        public static Builder builder(final ShellState shellState) {
            return new Builder(shellState);
        }

        /**
         * Builds instances of {@link ShellState}.
         */
        @ParametersAreNonnullByDefault
        public static class Builder {
            private ConnectionState connectionState;

            // Connection specific values.
            private AccumuloConnectionDetails connectionDetails;
            private RyaClient connectedCommands;

            // Instance specific values.
            private String instanceName;

            /**
             * Constructs an empty instance of {@link Builder}.
             */
            public Builder() { }

            /**
             * Constructs an instance of {@builder} initialized with the values
             * of a {@link ShellState}.
             *
             * @param shellState - The initial state of the builder. (not null)
             */
            public Builder(final ShellState shellState) {
                this.connectionState = shellState.getConnectionState();
                this.connectionDetails = shellState.getConnectionDetails().orNull();
                this.connectedCommands = shellState.getConnectedCommands().orNull();
                this.instanceName = shellState.getRyaInstanceName().orNull();
            }

            /**
             * @param connectionState - The {@link ConnectionState} of the Rya Shell.
             * @return This {@link Builder} so that method invocations may be chained.
             */
            public Builder setConnectionState(@Nullable final ConnectionState connectionState) {
                this.connectionState = connectionState;
                return this;
            }

            /**
             * @param connectionDetails - Metadata about the Accumulo connection.
             * @return This {@link Builder} so that method invocations may be chained.
             */
            public Builder setAccumuloConnectionDetails(@Nullable final AccumuloConnectionDetails connectionDetails) {
                this.connectionDetails = connectionDetails;
                return this;
            }

            /**
             * @param connectedCommands - The {@link RyaClient} to use when a command on the shell is issued.
             * @return This {@link Builder} so that method invocations may be chained.
             */
            public Builder setConnectedCommands(@Nullable final RyaClient connectedCommands) {
                this.connectedCommands = connectedCommands;
                return this;
            }

            /**
             * @param instanceName - The name of the Rya Instance the Rya Shell is issuing commands to.
             * @return This {@link Builder} so that method invocations may be chained.
             */
            public Builder setRyaInstanceName(@Nullable final String instanceName) {
                this.instanceName = instanceName;
                return this;
            }

            /**
             * @return An instance of {@link ShellState} built using this builder's values.
             */
            public ShellState build() {
                return new ShellState(
                        connectionState,
                        Optional.fromNullable(connectionDetails),
                        Optional.fromNullable(connectedCommands),
                        Optional.fromNullable(instanceName));
            }
        }
    }
}