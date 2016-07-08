package org.apache.rya.export.api.conf;

/**
 * Helper for creating the immutable application configuration.
 */
public class ConfigurationAdapter {
    /**
     * @param jConfig - The JAXB generated configuration.
     * @return The {@link MergeConfiguration} used in the application
     * @throws MergeConfigurationException
     */
    public static MergeConfiguration createConfig(final JAXBMergeConfiguration jConfig) throws MergeConfigurationException {
        final Builder configBuilder = new Builder()
        .setParentHostname(jConfig.getParentHostname())
        .setParentRyaInstanceName(jConfig.getParentRyaInstanceName())
        .setParentDBType(jConfig.getParentDBType())
        .setParentPort(jConfig.getParentPort())
        .setChildHostname(jConfig.getChildHostname())
        .setChildRyaInstanceName(jConfig.getChildRyaInstanceName())
        .setChildDBType(jConfig.getChildDBType())
        .setChildPort(jConfig.getChildPort())
        .setMergePolicy(jConfig.getMergePolicy());
        return configBuilder.build();
    }
}
