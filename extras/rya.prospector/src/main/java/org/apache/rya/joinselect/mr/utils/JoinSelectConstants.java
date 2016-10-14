package org.apache.rya.joinselect.mr.utils;

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



public class JoinSelectConstants {

  public static final String COUNT = "count";
  public static final String METADATA = "metadata";
  public static final byte[] EMPTY = new byte[0];

  // config properties
  public static final String PERFORMANT = "performant";
  public static final String USERNAME = "username";
  public static final String PASSWORD = "password";
  public static final String INSTANCE = "instance";
  public static final String ZOOKEEPERS = "zookeepers";
  public static final String INPUTPATH = "inputpath";
  public static final String OUTPUTPATH = "outputpath";
  public static final String PROSPECTS_OUTPUTPATH = "prospects.outputpath";
  public static final String SPO_OUTPUTPATH = "spo.outputpath";
  public static final String AUTHS = "auths";
  public static final String PROSPECTS_TABLE = "prospects.table";
  public static final String SPO_TABLE = "spo.table";
  public static final String SELECTIVITY_TABLE = "selectivity.table";
  public static final String MOCK = "mock";

}
