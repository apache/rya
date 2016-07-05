package mvm.rya.accumulo.mr;

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



import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;
import org.openrdf.model.Resource;
import org.openrdf.model.Statement;
import org.openrdf.model.URI;
import org.openrdf.model.Value;

import mvm.rya.indexing.StatementSerializer;

/**
 * A {@link Writable} wrapper for {@link Statement} objects.
 */
@SuppressWarnings("serial")
public class StatementWritable implements Statement, Writable {

    private Statement statement;

    public StatementWritable(Statement statement) {
        setStatement(statement);
    }

    public void setStatement(Statement statement) {
        this.statement = statement;
    }

    public Statement getStatement() {
        return statement;
    }

    @Override
    public void readFields(DataInput paramDataInput) throws IOException {
        statement = StatementSerializer.readStatement(paramDataInput.readUTF());
    }

    @Override
    public void write(DataOutput paramDataOutput) throws IOException {
        paramDataOutput.writeUTF(StatementSerializer.writeStatement(statement));
    }

    @Override
    public Resource getSubject() {
        return statement.getSubject();
    }

    @Override
    public URI getPredicate() {
        return statement.getPredicate();
    }

    @Override
    public Value getObject() {
        return statement.getObject();
    }

    @Override
    public Resource getContext() {
        return statement.getContext();
    }

}
