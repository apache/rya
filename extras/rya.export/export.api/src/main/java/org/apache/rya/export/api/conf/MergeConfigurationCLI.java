package org.apache.rya.export.api.conf;

import java.io.File;

import javax.xml.bind.JAXBContext;
import javax.xml.bind.JAXBException;
import javax.xml.bind.Unmarshaller;

import org.apache.commons.cli.BasicParser;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.rya.export.DBType;
import org.apache.rya.export.JAXBMergeConfiguration;
import org.apache.rya.export.MergePolicy;

import com.google.common.annotations.VisibleForTesting;

/**
 * Helper class for processing command line arguemnts for the Merge Tool.
 */
public class MergeConfigurationCLI {
    public static final Option CONFIG_OPTION = new Option("c", true, "Defines the configuration file for the Merge Tool to use.");

    /**
     * @return The valid {@link Options}
     */
    @VisibleForTesting
    public static Options getOptions() {
        final Options cliOptions = new Options()
        .addOption(CONFIG_OPTION);
        return cliOptions;
    }

    @VisibleForTesting
    public static JAXBMergeConfiguration createConfigurationFromFile(final File configFile) throws MergeConfigurationException {
        try {
            final JAXBContext context = JAXBContext.newInstance(DBType.class, JAXBMergeConfiguration.class, MergePolicy.class);
            final Unmarshaller unmarshaller = context.createUnmarshaller();
            return (JAXBMergeConfiguration) unmarshaller.unmarshal(configFile);
        } catch (final JAXBException | IllegalArgumentException JAXBe) {
            throw new MergeConfigurationException("Failed to create a config based on the provided configuration.", JAXBe);
        }
    }

    public static MergeConfiguration createConfiguration(final String[] args) throws MergeConfigurationException {
        final Options cliOptions = getOptions();
        final CommandLineParser parser = new BasicParser();
        try {
            final CommandLine cmd = parser.parse(cliOptions, args);
            //If the config option is present, ignore all other options.
            if(cmd.hasOption(CONFIG_OPTION.getOpt())) {
                final File xml = new File(cmd.getOptionValue(CONFIG_OPTION.getOpt()));
                return ConfigurationAdapter.createConfig(createConfigurationFromFile(xml));
            } else {
                throw new MergeConfigurationException("No configuration was provided.");
            }
        } catch (final ParseException pe) {
            throw new MergeConfigurationException("Improperly formatted options.", pe);
        }
    }
}
