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
package org.apache.rya.export.client.conf;

import static com.google.common.base.Preconditions.checkNotNull;

import java.io.File;
import java.io.IOException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;

import javax.xml.bind.JAXBContext;
import javax.xml.bind.JAXBException;
import javax.xml.bind.Unmarshaller;
import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;

import org.apache.commons.cli.BasicParser;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.rya.api.utils.XmlFactoryConfiguration;
import org.apache.rya.export.Accumulo;
import org.apache.rya.export.Connection;
import org.apache.rya.export.InstanceType;
import org.apache.rya.export.MergeToolConfiguration;
import org.apache.rya.export.Mongo;
import org.xml.sax.SAXException;

import com.google.common.annotations.VisibleForTesting;

/**
 * Helper class for processing command line arguments for the Merge Tool.
 */
public class MergeConfigurationCLI {
    private static final Option CONFIG_OPTION = new Option("c", true, "Defines the configuration file for the Merge Tool to use.");
    /**
     * Formatted in "MMM ddd yyy HH:mm:ss"
     */
    public static final DateFormat DATE_FORMAT = new SimpleDateFormat("MMM ddd yyy HH:mm:ss");
    private final CommandLine cmd;

    private MergeToolConfiguration configuration;
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
        .addOption(CONFIG_OPTION);
        return cliOptions;
    }

    public static MergeToolConfiguration createConfigurationFromFile(final File configFile) throws MergeConfigurationException {
        try {
            final JAXBContext context = JAXBContext.newInstance(MergeToolConfiguration.class, Mongo.class, Accumulo.class, Connection.class, InstanceType.class);
            final Unmarshaller unmarshaller = context.createUnmarshaller();
            final DocumentBuilderFactory dbf = DocumentBuilderFactory.newInstance();
            XmlFactoryConfiguration.harden(dbf);
            final DocumentBuilder db = dbf.newDocumentBuilder();
            return unmarshaller.unmarshal(db.parse(configFile), MergeToolConfiguration.class).getValue();
        } catch (final JAXBException | IllegalArgumentException | ParserConfigurationException | SAXException | IOException JAXBe) {
            throw new MergeConfigurationException("Failed to create a config based on the provided configuration.", JAXBe);
        }
    }

    /**
     * Attempts to create the {@link MergeConfiguration} based on the provided
     * configuration file.
     * @return The {@link MergeConfiguration} created.
     * @throws MergeConfigurationException - Thrown when the provided file is
     * not formatted properly for a {@link MergeConfiguration}.
     */
    public MergeToolConfiguration createConfiguration() throws MergeConfigurationException {
        if(configuration == null) {
            //If the config option is present, ignore all other options.
            if(cmd.hasOption(CONFIG_OPTION.getOpt())) {
                final File xml = new File(cmd.getOptionValue(CONFIG_OPTION.getOpt()));
                configuration = createConfigurationFromFile(xml);
            } else {
                throw new MergeConfigurationException("Must include a configuration file.");
            }
        }
        return configuration;
    }


}
