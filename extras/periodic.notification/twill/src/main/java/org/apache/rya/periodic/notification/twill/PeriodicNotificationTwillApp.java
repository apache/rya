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
package org.apache.rya.periodic.notification.twill;

import java.io.File;

import org.apache.twill.api.ResourceSpecification;
import org.apache.twill.api.ResourceSpecification.SizeUnit;
import org.apache.twill.api.TwillApplication;
import org.apache.twill.api.TwillSpecification;

public class PeriodicNotificationTwillApp implements TwillApplication {


    private final File configFile;
    public static final String APPLICATION_NAME = PeriodicNotificationTwillApp.class.getSimpleName();

    public PeriodicNotificationTwillApp(final File configFile) {
        this.configFile = configFile;
    }

    @Override
    public TwillSpecification configure() {
        return TwillSpecification.Builder.with()
                .setName(APPLICATION_NAME)
                .withRunnable()
                    .add(PeriodicNotificationTwillRunnable.TWILL_RUNNABLE_NAME,
                            new PeriodicNotificationTwillRunnable(),
                            ResourceSpecification.Builder.with()
                                .setVirtualCores(2)
                                .setMemory(2, SizeUnit.GIGA)
                                .setInstances(1)
                                .build())
                    .withLocalFiles()
                    .add(PeriodicNotificationTwillRunnable.CONFIG_FILE_NAME, configFile)
                    .apply()
                .anyOrder()
                .build();
    }

}
