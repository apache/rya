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

import java.io.File;

import org.springframework.core.Ordered;
import org.springframework.core.annotation.Order;
import org.springframework.shell.plugin.HistoryFileNameProvider;
import org.springframework.stereotype.Component;

/**
 * Customizes the Rya Shell's history file.
 */
@Component
@Order(Ordered.HIGHEST_PRECEDENCE)
public class RyaShellHistoryProvider implements HistoryFileNameProvider {

    public static final String RYA_SHELL_HISTORY_FILENAME = ".rya_shell_history";

    @Override
    public String getHistoryFileName() {
        final String userHome = System.getProperty("user.home");
        if(userHome == null) {
            return RYA_SHELL_HISTORY_FILENAME;
        } else {
            return new File(userHome, RYA_SHELL_HISTORY_FILENAME).getAbsolutePath();
        }
    }

    @Override
    public String getProviderName() {
        return this.getClass().getSimpleName();
    }
}