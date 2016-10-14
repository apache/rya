package org.apache.rya.api;

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
 * Class InvalidValueTypeMarkerRuntimeException
 * Date: Jan 7, 2011
 * Time: 12:58:27 PM
 */
public class InvalidValueTypeMarkerRuntimeException extends RuntimeException {
    private int valueTypeMarker = -1;

    public InvalidValueTypeMarkerRuntimeException(int valueTypeMarker) {
        super();
        this.valueTypeMarker = valueTypeMarker;
    }

    public InvalidValueTypeMarkerRuntimeException(int valueTypeMarker, String s) {
        super(s);
        this.valueTypeMarker = valueTypeMarker;
    }

    public InvalidValueTypeMarkerRuntimeException(int valueTypeMarker, String s, Throwable throwable) {
        super(s, throwable);
        this.valueTypeMarker = valueTypeMarker;
    }

    public InvalidValueTypeMarkerRuntimeException(int valueTypeMarker, Throwable throwable) {
        super(throwable);
        this.valueTypeMarker = valueTypeMarker;
    }

    public int getValueTypeMarker() {
        return valueTypeMarker;
    }
}
