package org.apache.rya.alx.command;

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



import org.apache.rya.api.RdfCloudTripleStoreConfiguration;
import org.apache.felix.gogo.commands.Command;

import java.util.Map;

/**
 * Date: 5/16/12
 * Time: 11:04 AM
 */
@Command(scope = "rya", name = "info", description = "Displays information about the running Rya instance")
public class InfoRyaCommand extends AbstractRyaCommand {

    @Override
    protected Object doRyaExecute() throws Exception {
        System.out.println("******************RYA Configuration******************");
        RdfCloudTripleStoreConfiguration conf = rdfDAO.getConf();
        for (Map.Entry<String, String> next : conf) {
            System.out.println(next.getKey() + ":\t\t" + next.getValue());
        }
        System.out.println("*****************************************************");
        return null;
    }
}
