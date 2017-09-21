#!/bin/bash

# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
# 
#   http://www.apache.org/licenses/LICENSE-2.0
# 
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

name="Mongo Rya Direct Example"
echo "Starting $name"

CP=""

# Check to see if javac is on the path
if which javac >/dev/null; then
    for f in ./lib/*.jar; do
        CP+="$f:"
    done

    echo "Compiling $name"
    javac -cp "$CP" MongoRyaDirectExample.java
    echo "Running $name"
    java -cp "$CP" MongoRyaDirectExample

    echo "Finished $name"
else
    echo "ERROR: Could not find javac"
fi
