package org.apache.rya.api.persist;

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



/**
 * Class RdfDAOException
 * Date: Feb 28, 2012
 * Time: 3:39:36 PM
 */
public class RdfDAOException extends RuntimeException {
    public RdfDAOException() {
    }

    public RdfDAOException(String s) {
        super(s);
    }

    public RdfDAOException(String s, Throwable throwable) {
        super(s, throwable);
    }

    public RdfDAOException(Throwable throwable) {
        super(throwable);
    }
}
