package org.apache.rya.rdftriplestore;

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



import org.openrdf.repository.RepositoryException;
import org.openrdf.repository.sail.SailRepository;
import org.openrdf.repository.sail.SailRepositoryConnection;
import org.openrdf.sail.Sail;
import org.openrdf.sail.SailException;

/**
 * Created by IntelliJ IDEA.
 * User: RoshanP
 * Date: 3/23/12
 * Time: 10:05 AM
 * To change this template use File | Settings | File Templates.
 */
public class RyaSailRepository extends SailRepository{
    public RyaSailRepository(Sail sail) {
        super(sail);
    }

    @Override
    public SailRepositoryConnection getConnection() throws RepositoryException {
        try
        {
            return new RyaSailRepositoryConnection(this, this.getSail().getConnection());
        }
        catch(SailException e)
        {
            throw new RepositoryException(e);
        }
    }
}
