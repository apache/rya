/*
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
package org.apache.rya.accumulo.mr.merge.util;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;

import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.XMLConfiguration;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.log4j.Logger;

/**
 * Utility methods for the merge tool and copy tool configuration files.
 */
public final class ToolConfigUtils {
    private static final Logger log = Logger.getLogger(ToolConfigUtils.class);

    /**
     * Private constructor to prevent instantiation.
     */
    private ToolConfigUtils() {
    }

    /**
     * Gets the set of user arguments from the user's config and/or their extra supplied
     * command line arguments.  This weeds out all the automatically generated parameters created
     * from initializing a {@link Configuration} object and should only give back a set of arguments
     * provided directly by the user.
     * @param conf the {@link Configuration} provided.
     * @param args the extra arguments from the command line.
     * @return a {@link Set} of argument strings.
     * @throws IOException
     */
    public static Set<String> getUserArguments(final Configuration conf, final String[] args) throws IOException {
        String[] filteredArgs = new String[] {};
        if (Arrays.asList(args).contains("-conf")) {
            // parse args
            new GenericOptionsParser(conf, args);

            final List<String> commandLineArgs = new ArrayList<>();
            for (final String arg : args) {
                if (arg.startsWith("-D")) {
                    commandLineArgs.add(arg);
                }
            }
            filteredArgs = commandLineArgs.toArray(new String[0]);
        } else {
            filteredArgs = args;
        }

        // Get the supplied config name from the resource string.
        // No real easy way of getting the name.
        // So, pulling it off the list of resource names in the Configuration's toString() method
        // where it should be the last one.
        final String confString = conf.toString();
        final String resourceString = StringUtils.removeStart(confString, "Configuration: ");
        final List<String> resourceNames = Arrays.asList(StringUtils.split(resourceString, ", "));
        final String configFilename = resourceNames.get(resourceNames.size() - 1);

        final Set<String> toolArgsSet = new HashSet<>();
        final File file = new File(configFilename);
        // Check that the last resource name is the actual user's config by seeing if it's a file
        // on the system, the other resources seem to be contained in jars and so should fail here which
        // should happen if no config is supplied.
        if (file.exists()) {
            XMLConfiguration configuration = null;
            try {
                configuration = new XMLConfiguration(configFilename);
                toolArgsSet.addAll(getConfigArguments(configuration));
            } catch (final ConfigurationException e) {
                log.error("Unable to load configuration file.", e);
            }
        }

        toolArgsSet.addAll(Arrays.asList(filteredArgs));
        return Collections.unmodifiableSet(toolArgsSet);
    }

    /**
     * Reads in the configuration file properties and values and converts them
     * into a set of argument strings.
     * @param configuration the {@link XMLConfiguration}.
     * @return the set of argument strings.
     */
    public static Set<String> getConfigArguments(final XMLConfiguration configuration) {
        final int size = configuration.getList("property.name").size();
        final TreeSet<String> configArgs = new TreeSet<>();
        for (int i = 0; i < size; i++) {
            final String propertyName = configuration.getString("property(" + i + ").name");
            final String propertyValue = configuration.getString("property(" + i + ").value");
            final String argument = makeArgument(propertyName, propertyValue);
            configArgs.add(argument);
        }
        return configArgs;
    }

    /**
     * Creates an argument string from the specified property name and value.
     * If the property name is "config.file" and value is "config.xml" then this will
     * create an argument string of "-Dconfig.file=config.xml"
     * @param propertyName the property name.
     * @param value the value.
     * @return the argument string.
     */
    public static String makeArgument(final String propertyName, final String value) {
        return "-D" + propertyName + "=" + value;
    }
}