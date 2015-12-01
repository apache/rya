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

package mvm.rya.blueprints.sail

import com.tinkerpop.blueprints.Edge
import info.aduna.iteration.Iteration
import info.aduna.iteration.Iterations
import info.aduna.iteration.IteratorIteration
import org.openrdf.model.Statement
import org.openrdf.sail.SailException

/**
 * Edge iterable that returns RyaSailEdge
 * Date: 5/9/12
 * Time: 9:26 AM
 */
class RyaSailEdgeSequence implements Iterable<Edge>, Iterator<Edge>
{

    protected Iteration<? extends Statement, SailException> statements;
    protected RyaSailGraph graph;
    
    public RyaSailEdgeSequence(Iteration statements, RyaSailGraph graph)
    {
        this.statements = statements;
        this.graph = graph;
    }

    public RyaSailEdgeSequence(Iterator iterator, RyaSailGraph graph) {
        this(new IteratorIteration(iterator), graph)
    }

    public RyaSailEdgeSequence()
    {
        statements = null;
        graph = null;
    }

    public Iterator iterator()
    {
        return this;
    }

    public void remove()
    {
        throw new UnsupportedOperationException();
    }

    public boolean hasNext()
    {
        if(null == statements)
            return false;
        try
        {
            if(statements.hasNext())
                return true;
        }
        catch(SailException e)
        {
            throw new RuntimeException(e.getMessage(), e);
        }
        Iterations.closeCloseable(statements);
        return false;
    }

    public Edge next()
    {
        if(null == statements)
            throw new NoSuchElementException();
        try
        {
            def statement = (Statement) statements.next()
            return new RyaSailEdge(statement, graph);
        }
        catch(SailException e)
        {
            throw new RuntimeException(e.getMessage());
        }
        catch(NoSuchElementException e)
        {
            try
            {
                Iterations.closeCloseable(statements);
            }
            catch(SailException e2)
            {
                throw new RuntimeException(e2.getMessage(), e2);
            }
            throw e;
        }
    }

}
