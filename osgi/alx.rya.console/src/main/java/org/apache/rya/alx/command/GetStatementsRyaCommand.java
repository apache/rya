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



import org.apache.felix.gogo.commands.Command;
import org.apache.felix.gogo.commands.Option;
import org.openrdf.model.Resource;
import org.openrdf.model.Statement;
import org.openrdf.model.URI;
import org.openrdf.repository.RepositoryConnection;
import org.openrdf.repository.RepositoryResult;

import static org.apache.rya.api.RdfCloudTripleStoreUtils.*;

/**
 * Date: 5/16/12
 * Time: 1:23 PM
 */
@Command(scope = "rya", name = "getstatements", description = "Print statements to screen based on triple pattern")
public class GetStatementsRyaCommand extends AbstractRyaCommand {
    @Option(name = "-s", aliases = {"--subject"}, description = "Subject of triple pattern", required = false, multiValued = false)
    private String subject;
    @Option(name = "-p", aliases = {"--predicate"}, description = "Predicate of triple pattern", required = false, multiValued = false)
    private String predicate;
    @Option(name = "-o", aliases = {"--object"}, description = "Object of triple pattern", required = false, multiValued = false)
    private String object;
    @Option(name = "-c", aliases = {"--context"}, description = "Context of triple pattern", required = false, multiValued = false)
    private String context;

    @Override
    protected Object doRyaExecute() throws Exception {
        if (subject == null && predicate == null && object == null && context == null) {
            System.out.println("Please specify subject|predicate|object|context");
            return null;
        }

        System.out.println(subject);
        System.out.println(predicate);
        System.out.println(object);
        System.out.println(context);
        RepositoryConnection connection = null;
        try {
            connection = repository.getConnection();
            RepositoryResult<Statement> statements = connection.getStatements(
                    (subject != null) ? (Resource) createValue(subject) : null,
                    (predicate != null) ? (URI) createValue(predicate) : null,
                    (object != null) ? createValue(object) : null,
                    false,
                    (context != null) ? new Resource[]{(Resource) createValue(context)} : new Resource[0]);
            while(statements.hasNext()) {
                System.out.println(statements.next());
            }
            statements.close();
        } finally {
            if (connection != null) {
                connection.close();
            }
        }
        return null;
    }
}
