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
package org.apache.rya.export.api.conf;

import static com.google.common.base.Preconditions.checkNotNull;

import java.io.File;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;

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
import org.apache.rya.export.api.conf.ConfigurationAdapter;
import org.apache.rya.export.api.conf.MergeConfiguration;
import org.apache.rya.export.api.conf.MergeConfigurationException;
import org.apache.rya.export.client.gui.DateTimePickerDialog;

import com.google.common.annotations.VisibleForTesting;

/**
 * Helper class for processing command line arguments for the Merge Tool.
 */
public class MergeConfigurationCLI {
    public static final Option CONFIG_OPTION = new Option("c", true, "Defines the configuration file for the Merge Tool to use.");
    public static final Option TIME_OPTION = new Option("t", true, "Defines the timestamp from which to filter RyaStatements when merging.");
    private static final DateFormat DATE_FORMAT = new SimpleDateFormat("MMM ddd yyy HH:mm:ss");
    private final CommandLine cmd;
    /**
     *
     * @param args
     * @throws MergeConfigurationException
     */
    public MergeConfigurationCLI(final String[] args) throws MergeConfigurationException {
        checkNotNull(args);

        final Options cliOptions = getOptions();
        final CommandLineParser parser = new BasicParser();
        try {
            cmd = parser.parse(cliOptions, args);
        } catch (final ParseException pe) {
            throw new MergeConfigurationException("Improperly formatted options.", pe);
        }
    }

    /**
     * @return The valid {@link Options}
     */
    @VisibleForTesting
    public static Options getOptions() {
        final Options cliOptions = new Options()
        .addOption(TIME_OPTION)
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

    public Date getRyaStatementMergeTime() throws MergeConfigurationException {
        final Date time;
        if(cmd.hasOption(TIME_OPTION.getOpt())) {
            final String dateStr = cmd.getOptionValue(TIME_OPTION.getOpt());
            try {
                time = DATE_FORMAT.parse(dateStr);
            } catch (final java.text.ParseException e) {
                throw new MergeConfigurationException("The provided timestamp was not formatted correctly.", e);
            }
        } else {
            //clean up this message.
            final DateTimePickerDialog dialog = new DateTimePickerDialog("Merge Time Selection", "Select the timestamp in which merging will occur.");
            dialog.setVisible(true);
            time = dialog.getSelectedDateTime();
        }
        return time;
    }

    /**
     *
     * @return
     * @throws MergeConfigurationException
     */
    public MergeConfiguration createConfiguration() throws MergeConfigurationException {
        //If the config option is present, ignore all other options.
        if(cmd.hasOption(CONFIG_OPTION.getOpt())) {
            final File xml = new File(cmd.getOptionValue(CONFIG_OPTION.getOpt()));
            return ConfigurationAdapter.createConfig(createConfigurationFromFile(xml));
        } else {
            throw new MergeConfigurationException("No configuration was provided.");
        }
    }
}
