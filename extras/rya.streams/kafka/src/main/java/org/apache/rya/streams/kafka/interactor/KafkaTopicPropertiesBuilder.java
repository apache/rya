package org.apache.rya.streams.kafka.interactor;

import static java.util.Objects.requireNonNull;

import java.util.Optional;
import java.util.Properties;

/**
 * Properties builder to be used when creating new Kafka Topics.
 *
 * Descriptions of properties can be found at
 * {@link https://kafka.apache.org/documentation/#topicconfigs}
 */
public class KafkaTopicPropertiesBuilder {
    /*----- Cleanup Policy -----*/
    public static final String CLEANUP_POLICY_KEY = "cleanup.policy";
    public static final String CLEANUP_POLICY_DELETE = "cleanup.policy";
    public static final String CLEANUP_POLICY_COMPACT = "cleanup.policy";


    private Optional<String> cleanupPolicy;
    /**
     * Sets the cleanup.policy of the Kafka Topic.
     *
     * @param policy - The cleanup policy to use.
     * @return The builder.
     */
    public KafkaTopicPropertiesBuilder setCleanupPolicy(final String policy) {
        cleanupPolicy = Optional.of(requireNonNull(policy));
        return this;
    }

    /**
     * Builds the Kafka topic properties.
     * @return The {@link Properties} of the Kafka Topic.
     */
    public Properties build() {
        final Properties props = new Properties();

        if(cleanupPolicy.isPresent()) {
            props.setProperty(CLEANUP_POLICY_KEY, cleanupPolicy.get());
        }

        return props;
    }
}
