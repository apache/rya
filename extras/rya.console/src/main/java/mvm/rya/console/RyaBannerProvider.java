package mvm.rya.console;

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


import org.springframework.core.Ordered;
import org.springframework.core.annotation.Order;
import org.springframework.shell.core.CommandMarker;
import org.springframework.shell.core.annotation.CliCommand;
import org.springframework.shell.plugin.support.DefaultBannerProvider;
import org.springframework.shell.support.util.OsUtils;
import org.springframework.stereotype.Component;

/**
 * @author Jarred Li
 */
@Component
@Order(Ordered.HIGHEST_PRECEDENCE)
public class RyaBannerProvider extends DefaultBannerProvider
        implements CommandMarker {

    @CliCommand(value = {"version"}, help = "Displays current CLI version")
    @Override
    public String getBanner() {
        StringBuffer buf = new StringBuffer();
        buf.append("" +
                "________                    _________                         ______     \n" +
                "___  __ \\____  _______ _    __  ____/____________________________  /____ \n" +
                "__  /_/ /_  / / /  __ `/    _  /    _  __ \\_  __ \\_  ___/  __ \\_  /_  _ \\\n" +
                "_  _, _/_  /_/ // /_/ /     / /___  / /_/ /  / / /(__  )/ /_/ /  / /  __/\n" +
                "/_/ |_| _\\__, / \\__,_/      \\____/  \\____//_/ /_//____/ \\____//_/  \\___/ \n" +
                "        /____/ " + OsUtils.LINE_SEPARATOR);
        buf.append("Version:" + this.getVersion());
        return buf.toString();

    }

    @Override
    public String getVersion() {
        return "3.0.0";
    }

    @Override
    public String getWelcomeMessage() {
        return "Welcome to the Rya Console";
    }

    @Override
    public String getProviderName() {
        return "rya";
    }
}
