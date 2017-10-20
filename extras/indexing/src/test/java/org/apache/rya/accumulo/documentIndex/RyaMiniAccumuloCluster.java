package org.apache.rya.accumulo.documentIndex;

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

import java.io.File;
import java.io.IOException;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.TableExistsException;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.core.security.TablePermission;
import org.apache.accumulo.minicluster.MiniAccumuloCluster;
import org.apache.accumulo.minicluster.MiniAccumuloConfig;
import org.apache.commons.io.FileUtils;

/**
 * Serves as entry point for using MiniAccumulo cluster.
 */
public class RyaMiniAccumuloCluster {

    private Connector adminConn;
    private boolean usersCreated=false;

    private MiniAccumuloCluster accMiniCluster;

    protected RyaMiniAccumuloCluster() throws IOException, InterruptedException, AccumuloSecurityException, AccumuloException{

            File loc = new File("./target/minicluster");
            FileUtils.deleteDirectory(loc);
            loc.mkdir();
            loc.deleteOnExit();
            MiniAccumuloConfig accMiniClusterConfig =  new MiniAccumuloConfig(loc, "safe");
            accMiniClusterConfig.setZooKeeperStartupTime(30000);
            accMiniCluster = new MiniAccumuloCluster(accMiniClusterConfig);
            accMiniCluster.start();
            adminConn = accMiniCluster.getConnector("root", "safe");
    }

    public Connector getAdminConn() {
        return adminConn;
    }

    public void stop() throws IOException, InterruptedException {
        accMiniCluster.stop();
    }

    public void createTableAndUsers(String tablename, Authorizations auths)
            throws TableExistsException, AccumuloSecurityException, AccumuloException
    {
        if(!usersCreated) {
            adminConn.tableOperations().create(tablename);
            adminConn.securityOperations().createLocalUser("user1", new PasswordToken("user1"));
            adminConn.securityOperations().changeUserAuthorizations("user1", auths);
            adminConn.securityOperations().grantTablePermission("user1", tablename, TablePermission.WRITE);
            adminConn.securityOperations().grantTablePermission("user1", tablename, TablePermission.READ);
            usersCreated=true;
        }

    }
}
