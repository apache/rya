/*
 * Copyright 2011-2012 the original author or authors.
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *      http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package mvm.rya.console;

import org.springframework.core.Ordered;
import org.springframework.core.annotation.Order;
import org.springframework.shell.support.util.StringUtils;
import org.springframework.shell.core.CommandMarker;
import org.springframework.shell.core.annotation.CliCommand;
import org.springframework.shell.plugin.support.DefaultBannerProvider;
import org.springframework.stereotype.Component;

/**
 * @author Jarred Li
 */
@Component
@Order(Ordered.HIGHEST_PRECEDENCE)
public class RyaBannerProvider extends DefaultBannerProvider
        implements CommandMarker {

    @CliCommand(value = {"version"}, help = "Displays current CLI version")
    public String getBanner() {
        StringBuffer buf = new StringBuffer();
        buf.append("" +
                "________                    _________                         ______     \n" +
                "___  __ \\____  _______ _    __  ____/____________________________  /____ \n" +
                "__  /_/ /_  / / /  __ `/    _  /    _  __ \\_  __ \\_  ___/  __ \\_  /_  _ \\\n" +
                "_  _, _/_  /_/ // /_/ /     / /___  / /_/ /  / / /(__  )/ /_/ /  / /  __/\n" +
                "/_/ |_| _\\__, / \\__,_/      \\____/  \\____//_/ /_//____/ \\____//_/  \\___/ \n" +
                "        /____/ " + StringUtils.LINE_SEPARATOR);
        buf.append("Version:" + this.getVersion());
        return buf.toString();

    }

    public String getVersion() {
        return "3.0.0";
    }

    public String getWelcomeMessage() {
        return "Welcome to the Rya Console";
    }

    @Override
    public String name() {
        return "rya";
    }
}