package org.apache.rya.export.client;

import java.io.File;

import org.apache.commons.cli.ParseException;
import org.apache.commons.lang.StringUtils;
import org.apache.log4j.BasicConfigurator;
import org.apache.log4j.Logger;
import org.apache.log4j.xml.DOMConfigurator;
import org.apache.rya.export.api.conf.MergeConfiguration;
import org.apache.rya.export.api.conf.MergeConfigurationCLI;
import org.apache.rya.export.api.conf.MergeConfigurationException;

/**
 * Drives the MergeTool.
 */
public class MergeDriverCLI {
    private static final Logger LOG = Logger.getLogger(MergeDriverCLI.class);
    private static MergeConfiguration configuration;

    public static void main(final String [] args) throws ParseException {
        final String log4jConfiguration = System.getProperties().getProperty("log4j.configuration");
        if (StringUtils.isNotBlank(log4jConfiguration)) {
            final String parsedConfiguration = StringUtils.removeStart(log4jConfiguration, "file:");
            final File configFile = new File(parsedConfiguration);
            if (configFile.exists()) {
                DOMConfigurator.configure(parsedConfiguration);
            } else {
                BasicConfigurator.configure();
            }
        }
        try {
            configuration = MergeConfigurationCLI.createConfiguration(args);
        } catch (final MergeConfigurationException e) {
            LOG.error("Configuration failed.", e);
        }

        if(configuration.getParentDBType() == ACCUMULO && configuration.getChildDBType() == ACCUMULO) {
            //do traditional Mergetool shenanigans
        } else {

        }

        LOG.info("Starting Merge Tool");

        Thread.setDefaultUncaughtExceptionHandler(new Thread.UncaughtExceptionHandler() {
            @Override
            public void uncaughtException(final Thread thread, final Throwable throwable) {
                LOG.error("Uncaught exception in " + thread.getName(), throwable);
            }
        });

        //final int returnCode = setupAndRun(args);

        LOG.info("Finished running Merge Tool");
        System.exit(1);
    }
}
