@echo off
rem Licensed to the Apache Software Foundation (ASF) under one
rem or more contributor license agreements.  See the NOTICE file
rem distributed with this work for additional information
rem regarding copyright ownership.  The ASF licenses this file
rem to you under the Apache License, Version 2.0 (the
rem "License"); you may not use this file except in compliance
rem with the License.  You may obtain a copy of the License at
rem 
rem   http://www.apache.org/licenses/LICENSE-2.0
rem 
rem Unless required by applicable law or agreed to in writing,
rem software distributed under the License is distributed on an
rem "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
rem KIND, either express or implied.  See the License for the
rem specific language governing permissions and limitations
rem under the License.
SET CP=

REM Check to see if javac is on the path
where /Q javac
IF %ERRORLEVEL% NEQ 0 goto :NO_JAVAC


for /f %%f in ('DIR /b .\lib\*.jar') do call :append .\lib\%%f

javac -cp "%CP%" RyaDirectExample.java
java -cp "%CP%" RyaDirectExample

goto :end

:append
@echo off
SET CP=%CP%%1;
goto :end

:NO_JAVAC
echo ERROR: Could not find javac
goto :end

:end
