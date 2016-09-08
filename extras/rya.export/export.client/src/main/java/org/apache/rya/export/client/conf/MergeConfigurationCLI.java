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
import static org.apache.rya.export.MergePolicy.TIMESTAMP;

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
import org.apache.rya.export.AccumuloMergeToolConfiguration;
import org.apache.rya.export.DBType;
import org.apache.rya.export.InstanceType;
import org.apache.rya.export.MergePolicy;
import org.apache.rya.export.MergeToolConfiguration;
import org.apache.rya.export.TimestampMergePolicyConfiguration;
import org.apache.rya.export.api.conf.AccumuloMergeConfiguration;
import org.apache.rya.export.api.conf.ConfigurationAdapter;
import org.apache.rya.export.api.conf.MergeConfiguration;
import org.apache.rya.export.api.conf.MergeConfigurationException;
import org.apache.rya.export.api.conf.policy.TimestampPolicyMergeConfiguration;

import com.google.common.annotations.VisibleForTesting;

/**
 * Helper class for processing command line arguments for the Merge Tool.
 */
public class MergeConfigurationCLI {
    private static final String DIALOG_TITLE = "Select a Start Time/Date";
    private static final String DIALOG_MESSAGE =
        "<html>Choose the time of the data to merge.<br>Only data modified AFTER the selected time will be merged.</html>";

    private static final Option CONFIG_OPTION = new Option("c", true, "Defines the configuration file for the Merge Tool to use.");
    private static final Option TIME_OPTION = new Option("t", true, "Defines the timestamp from which to filter RyaStatements when merging.");
    private static final Option PARENT_HOST_OPTION = new Option("a", "pHost", true, "Defines the hostname of the parent db to connect to.");
    private static final Option PARENT_USER_OPTION = new Option("b", "pUser", true, "Defines the username to connect to the parent DB.");
    private static final Option PARENT_PSWD_OPTION = new Option("d", "pPswd", true, "Defines the password to connect to the parent DB.");
    private static final Option PARENT_RYA_OPTION = new Option("e", "pRya", true, "Defines the rya instance name of the parent DB.");
    private static final Option PARENT_PREFIX_OPTION = new Option("f", "pPrefix", true, "Defines the table prefix of the parent DB.");
    private static final Option PARENT_TOMCAT_OPTION = new Option("g", "pTomcat", true, "Defines the location of Tomcat for the parent DB.");
    private static final Option PARENT_DB_OPTION = new Option("h", "pDB", true, "Defines the type of database the parent is.");
    private static final Option PARENT_PORT_OPTION = new Option("i", "pPort", true, "Defines the port of the parent DB to connect to.");
    private static final Option PARENT_ACCUMULO_ZOOKEEPERS_OPTION = new Option("j", "paZookeepers", true, "Defines the location of the zookeepers.");
    private static final Option PARENT_ACCUMULO_AUTHS_OPTION = new Option("k", "paAuths", true, "Defines the authorization level of the user.");
    private static final Option PARENT_ACCUMULO_TYPE_OPTION = new Option("l", "paType", true, "Defines the type of accumulo to connect to.");
    private static final Option CHILD_HOST_OPTION = new Option("m", "cHost", true, "Defines the hostname of the child db to connect to.");
    private static final Option CHILD_USER_OPTION = new Option("n", "cUser", true, "Defines the username to connect to the child DB.");
    private static final Option CHILD_PSWD_OPTION = new Option("o", "cPswd", true, "Defines the password to connect to the child DB.");
    private static final Option CHILD_RYA_OPTION = new Option("p", "cRya", true, "Defines the rya instance name of the child DB.");
    private static final Option CHILD_PREFIX_OPTION = new Option("q", "cPrefix", true, "Defines the table prefix of the child DB.");
    private static final Option CHILD_TOMCAT_OPTION = new Option("r", "cTomcat", true, "Defines the location of Tomcat for the child DB.");
    private static final Option CHILD_DB_OPTION = new Option("s", "cDB", true, "Defines the type of database the child is.");
    private static final Option CHILD_PORT_OPTION = new Option("u", "cPort", true, "Defines the port of the child DB to connect to.");
    private static final Option CHILD_ACCUMULO_ZOOKEEPERS_OPTION = new Option("v", "caZookeepers", true, "Defines the location of the zookeepers.");
    private static final Option CHILD_ACCUMULO_AUTHS_OPTION = new Option("w", "caAuths", true, "Defines the authorization level of the user.");
    private static final Option CHILD_ACCUMULO_TYPE_OPTION = new Option("x", "caType", true, "Defines the type of accumulo to connect to.");
    private static final Option MERGE_OPTION = new Option("y", "merge", true, "Defines the type of merging that should occur.");
    private static final Option NTP_OPTION = new Option("z", "useNTP", true, "Defines if NTP should be used to synch time.");
    public static final DateFormat DATE_FORMAT = new SimpleDateFormat("MMM ddd yyy HH:mm:ss");
    private final CommandLine cmd;

    private MergeConfiguration configuration;
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
        .addOption(CONFIG_OPTION)
        .addOption(PARENT_DB_OPTION)
        .addOption(PARENT_HOST_OPTION)
        .addOption(PARENT_PORT_OPTION)
        .addOption(PARENT_PREFIX_OPTION)
        .addOption(PARENT_PSWD_OPTION)
        .addOption(PARENT_RYA_OPTION)
        .addOption(PARENT_TOMCAT_OPTION)
        .addOption(PARENT_USER_OPTION)
        .addOption(PARENT_ACCUMULO_AUTHS_OPTION)
        .addOption(PARENT_ACCUMULO_TYPE_OPTION)
        .addOption(PARENT_ACCUMULO_ZOOKEEPERS_OPTION)
        .addOption(CHILD_DB_OPTION)
        .addOption(CHILD_HOST_OPTION)
        .addOption(CHILD_PORT_OPTION)
        .addOption(CHILD_PREFIX_OPTION)
        .addOption(CHILD_PSWD_OPTION)
        .addOption(CHILD_RYA_OPTION)
        .addOption(CHILD_TOMCAT_OPTION)
        .addOption(CHILD_USER_OPTION)
        .addOption(CHILD_ACCUMULO_AUTHS_OPTION)
        .addOption(CHILD_ACCUMULO_TYPE_OPTION)
        .addOption(CHILD_ACCUMULO_ZOOKEEPERS_OPTION)
        .addOption(MERGE_OPTION)
        .addOption(NTP_OPTION);
        return cliOptions;
    }

    public static MergeToolConfiguration createConfigurationFromFile(final File configFile) throws MergeConfigurationException {
        try {
            final JAXBContext context = JAXBContext.newInstance(DBType.class, MergeToolConfiguration.class, AccumuloMergeToolConfiguration.class, TimestampMergePolicyConfiguration.class, MergePolicy.class, InstanceType.class);
            final Unmarshaller unmarshaller = context.createUnmarshaller();
            return (MergeToolConfiguration) unmarshaller.unmarshal(configFile);
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
            final DateTimePickerDialog dialog = new DateTimePickerDialog(DIALOG_TITLE, DIALOG_MESSAGE);
            dialog.setVisible(true);
            time = dialog.getSelectedDateTime();
        }
        return time;
    }

    /**
     * Attempts to create the {@link MergeConfiguration} based on the provided
     * configuration file.
     * @return The {@link MergeConfiguration} created.
     * @throws MergeConfigurationException - Thrown when the provided file is
     * not formatted properly for a {@link MergeConfiguration}.
     */
    public MergeConfiguration createConfiguration() throws MergeConfigurationException {
        if(configuration == null) {
            //If the config option is present, ignore all other options.
            if(cmd.hasOption(CONFIG_OPTION.getOpt())) {
                final File xml = new File(cmd.getOptionValue(CONFIG_OPTION.getOpt()));
                final ConfigurationAdapter adapter = new ConfigurationAdapter();
                configuration = adapter.createConfig(createConfigurationFromFile(xml));
            } else {
                final DBType parentType = DBType.fromValue(cmd.getOptionValue(PARENT_DB_OPTION.getLongOpt()));
                final DBType childType = DBType.fromValue(cmd.getOptionValue(CHILD_DB_OPTION.getLongOpt()));
                final MergePolicy mergePolicy = MergePolicy.fromValue(cmd.getOptionValue(MERGE_OPTION.getLongOpt()));
                MergeConfiguration.Builder builder = new MergeConfiguration.Builder()
                    .setParentHostname(cmd.getOptionValue(PARENT_HOST_OPTION.getLongOpt()))
                    .setParentUsername(cmd.getOptionValue(PARENT_USER_OPTION.getLongOpt()))
                    .setParentPassword(cmd.getOptionValue(PARENT_PSWD_OPTION.getLongOpt()))
                    .setParentRyaInstanceName(cmd.getOptionValue(PARENT_RYA_OPTION.getLongOpt()))
                    .setParentTablePrefix(cmd.getOptionValue(PARENT_PREFIX_OPTION.getLongOpt()))
                    .setParentTomcatUrl(cmd.getOptionValue(PARENT_TOMCAT_OPTION.getLongOpt()))
                    .setParentDBType(parentType)
                    .setParentPort(Integer.parseInt(cmd.getOptionValue(PARENT_PORT_OPTION.getLongOpt())))
                    .setChildHostname(cmd.getOptionValue(CHILD_HOST_OPTION.getLongOpt()))
                    .setChildUsername(cmd.getOptionValue(CHILD_USER_OPTION.getLongOpt()))
                    .setChildPassword(cmd.getOptionValue(CHILD_PSWD_OPTION.getLongOpt()))
                    .setChildRyaInstanceName(cmd.getOptionValue(CHILD_RYA_OPTION.getLongOpt()))
                    .setChildTablePrefix(cmd.getOptionValue(CHILD_PREFIX_OPTION.getLongOpt()))
                    .setChildTomcatUrl(cmd.getOptionValue(CHILD_TOMCAT_OPTION.getLongOpt()))
                    .setChildDBType(childType)
                    .setChildPort(Integer.parseInt(cmd.getOptionValue(CHILD_PORT_OPTION.getLongOpt())))
                    .setMergePolicy(mergePolicy);
                if (mergePolicy == TIMESTAMP) {
                    builder = new TimestampPolicyMergeConfiguration.TimestampPolicyBuilder(builder)
                        .setToolStartTime(cmd.getOptionValue(TIME_OPTION.getLongOpt()));
                }
                if (parentType == DBType.ACCUMULO) {
                    builder = new AccumuloMergeConfiguration.AccumuloBuilder(builder)
                        .setParentZookeepers(cmd.getOptionValue(PARENT_ACCUMULO_ZOOKEEPERS_OPTION.getLongOpt()))
                        .setParentAuths(cmd.getOptionValue(PARENT_ACCUMULO_AUTHS_OPTION.getLongOpt()))
                        .setParentInstanceType(InstanceType.fromValue(cmd.getOptionValue(PARENT_ACCUMULO_TYPE_OPTION.getLongOpt())));
                }
                if (childType == DBType.ACCUMULO) {
                    builder = new AccumuloMergeConfiguration.AccumuloBuilder(builder)
                        .setChildZookeepers(cmd.getOptionValue(CHILD_ACCUMULO_ZOOKEEPERS_OPTION.getLongOpt()))
                        .setChildAuths(cmd.getOptionValue(CHILD_ACCUMULO_AUTHS_OPTION.getLongOpt()))
                        .setChildInstanceType(InstanceType.fromValue(cmd.getOptionValue(CHILD_ACCUMULO_TYPE_OPTION.getLongOpt())));
                }
                configuration = builder.build();
            }
        }
        return configuration;
    }
}
