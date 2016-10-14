:: Licensed to the Apache Software Foundation (ASF) under one
:: or more contributor license agreements.  See the NOTICE file
:: distributed with this work for additional information
:: regarding copyright ownership.  The ASF licenses this file
:: to you under the Apache License, Version 2.0 (the
:: "License"); you may not use this file except in compliance
:: with the License.  You may obtain a copy of the License at
::
::  http://www.apache.org/licenses/LICENSE-2.0
::
:: Unless required by applicable law or agreed to in writing,
:: software distributed under the License is distributed on an
:: "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
:: KIND, either express or implied.  See the License for the
:: specific language governing permissions and limitations
:: under the License.

@echo off

echo "Launching Copy Tool..."

:: Using a wildcard in the jar filename may not work in some Windows environments,
:: so use a hard-coded filename for the jar if necessary.
SET JAR_NAME=rya.merger-*-shaded.jar

hadoop jar %JAR_NAME% org.apache.rya.accumulo.mr.merge.CopyTool -conf config/copy_tool_configuration.xml