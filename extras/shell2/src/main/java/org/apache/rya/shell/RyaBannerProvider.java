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

import java.io.IOException;
import java.net.JarURLConnection;
import java.net.URL;
import java.util.jar.Attributes;
import java.util.jar.Manifest;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.core.Ordered;
import org.springframework.core.annotation.Order;
import org.springframework.shell.core.CommandMarker;
import org.springframework.shell.plugin.support.DefaultBannerProvider;
import org.springframework.stereotype.Component;

/**
 * Customizes the Rya Shell's banner.
 */
@Component
@Order(Ordered.HIGHEST_PRECEDENCE)
public class RyaBannerProvider extends DefaultBannerProvider implements CommandMarker {
    private final Logger log = LoggerFactory.getLogger(RyaBannerProvider.class);

    private static final String BANNER =
    " _____                _____ _          _ _ \n" +
    "|  __ \\              / ____| |        | | |\n" +
    "| |__) |   _  __ _  | (___ | |__   ___| | |\n" +
    "|  _  / | | |/ _` |  \\___ \\| '_ \\ / _ \\ | |\n" +
    "| | \\ \\ |_| | (_| |  ____) | | | |  __/ | |\n" +
    "|_|  \\_\\__, |\\__,_| |_____/|_| |_|\\___|_|_|\n" +
    "        __/ |                              \n" +
    "       |___/                               ";

    private String version = null;

    @Override
    public String getBanner() {
        return BANNER + "\n" + getVersion() + "\n";
    }

    @Override
    public String getWelcomeMessage() {
        return "Welcome to the Rya Shell.\n" +
                "\n" +
                "Execute one of the connect commands to start interacting with an instance of Rya.\n" +
                "You may press tab at any time to see which of the commands are available.";
    }

    @Override
    public String getVersion() {
        if(version == null) {
            version = loadVersion();
        }
        return version;
    }

    /**
     * Loads the version number from the Rya Shell's MANIFEST.MF file.
     *
     * @return The version number of the Rya Shell.
     */
    private String loadVersion() {
        final String className = getClass().getSimpleName() + ".class";
        final String classPath = getClass().getResource( className ).toString();

        try {
            final URL classUrl = new URL(classPath);
            final JarURLConnection jarConnection = (JarURLConnection) classUrl.openConnection();
            final Manifest manifest = jarConnection.getManifest();
            final Attributes attributes = manifest.getMainAttributes();
            return attributes.getValue("Implementation-Version");
        } catch (final IOException e) {
            log.error("Could not load the application's version from it's manifest.", e);
        }

        return "UNKNOWN";
    }
}