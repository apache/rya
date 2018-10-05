package org.apache.rya.export.client;

import java.awt.Dimension;
import java.awt.Toolkit;
import java.io.File;
import java.io.IOException;
import java.text.ParseException;

import javax.swing.JFrame;
import javax.swing.UIManager;
import javax.swing.plaf.nimbus.NimbusLookAndFeel;

import org.apache.commons.lang.StringUtils;
import org.apache.log4j.BasicConfigurator;
import org.apache.log4j.Logger;
import org.apache.log4j.xml.DOMConfigurator;
import org.apache.rya.api.path.PathUtils;
import org.apache.rya.export.MergeToolConfiguration;
import org.apache.rya.export.api.store.RyaStatementStore;
import org.apache.rya.export.client.conf.MergeConfigurationCLI;
import org.apache.rya.export.client.conf.MergeConfigurationException;
import org.apache.rya.export.client.conf.TimeUtils;
import org.apache.rya.export.client.merge.StatementStoreFactory;
import org.apache.rya.export.client.view.RepSynchToolPane;

import com.google.common.base.Optional;

/**
 * GUI based entry point into the rep/synch tool.
 */
public class RepSynchDriver {
    private static final long serialVersionUID = 1L;
    private static final Logger LOG = Logger.getLogger(RepSynchDriver.class);
    private static MergeToolConfiguration configuration;

    public static void main(final String[] args) throws MergeConfigurationException, ParseException {
        //configure logging
        final String log4jConfiguration = System.getProperties().getProperty("log4j.configuration");
        if (StringUtils.isNotBlank(log4jConfiguration)) {
            final String parsedConfiguration = PathUtils.clean(StringUtils.removeStart(log4jConfiguration, "file:"));
            final File configFile = new File(parsedConfiguration);
            if (configFile.exists()) {
                DOMConfigurator.configure(parsedConfiguration);
            } else {
                BasicConfigurator.configure();
            }
        }

        //setup rep/synch configs.
        final MergeConfigurationCLI config = new MergeConfigurationCLI(args);
        try {
            configuration = config.createConfiguration();
        } catch (final MergeConfigurationException e) {
            LOG.error("Configuration failed.", e);
        }

        final boolean useTimeSync = configuration.isUseNtpServer();
        Optional<Long> offset = Optional.absent();
        if (useTimeSync) {
            final String tomcat = configuration.getTomcatUrl();
            final String ntpHost = configuration.getNtpServerHost();
            try {
                offset = Optional.fromNullable(TimeUtils.getNtpServerAndMachineTimeDifference(ntpHost, tomcat));
            } catch (final IOException e) {
                LOG.error("Unable to get time difference between time server: " + ntpHost + " and the server: " + tomcat, e);
            }
        }

        //create statement stores
        final StatementStoreFactory storeFactory = new StatementStoreFactory(configuration);
        try {
            final RyaStatementStore parentStore = storeFactory.getStatementStore(configuration.getParent());
            final RyaStatementStore childStore = storeFactory.getStatementStore(configuration.getChild());

            final Long timeOffset;
            if (offset.isPresent()) {
                timeOffset = offset.get();
            } else {
                timeOffset = 0L;
            }

            LOG.info("Starting Merge Tool");

            //Setup view
            UIManager.setLookAndFeel(new NimbusLookAndFeel());

            final RepSynchToolPane repSynchPane = new RepSynchToolPane(parentStore, childStore, configuration.getParent().getRyaInstanceName(), timeOffset);
            final Dimension largeDim = new Dimension(600, 500);
            final Dimension smallDim = new Dimension(600, 150);
            final JFrame frame = new JFrame();
            frame.setContentPane(repSynchPane);
            frame.setVisible(true);
            frame.setSize(smallDim);
            frame.setTitle("Rya Replication/Synchronization Tool");
            frame.setDefaultCloseOperation(JFrame.EXIT_ON_CLOSE);
            //frame.setIconImage(/*rya icon!*/);
            repSynchPane.addResizeListener(logShown -> {
                if(logShown) {
                    frame.setSize(largeDim);
                    frame.repaint();
                } else {
                    frame.setSize(smallDim);
                    frame.repaint();
                }
            });

            final Dimension screen = Toolkit.getDefaultToolkit().getScreenSize();
            frame.setLocation(
                    (int)Math.round(screen.getHeight() / 2 - 250),
                    (int)Math.round(screen.getHeight() / 2 - 150));

        } catch (final Exception e) {
            LOG.error("Something went wrong creating a Rya Statement Store connection.", e);
        }
        Thread.setDefaultUncaughtExceptionHandler((thread, throwable) -> LOG.error("Uncaught exception in " + thread.getName(), throwable));
    }
}
