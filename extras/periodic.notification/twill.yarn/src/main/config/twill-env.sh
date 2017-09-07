# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.


#addToClasspath() 
#{
#  local dir=$1
#  local filterRegex=$2


#  if [ ! -d "$dir" ]; then
#    echo "ERROR $dir does not exist or not a directory"
#    exit 1
#  fi

#  for jar in $dir/*.jar; do
#    if ! [[ $jar =~ $filterRegex ]]; then
#       CLASSPATH="$CLASSPATH:$jar"
#    fi
#  done
#}

loadLocalAHZLibraries()
{
# Specify a hadoop configuration directory to use for connecting to the YARN cluster.
# At a minimum, this directory should contain core-site.xml and yarn-site.xml.
#HADOOP_CONF_DIR=/etc/hadoop/conf
HADOOP_CONF_DIR=conf/hadoop

# use local libraries (lib-ahz) for the classpath and append the hadoop conf directory
TWILL_CP=lib/*:lib-ahz/*:$HADOOP_CONF_DIR
}

loadSystemAHZLibraries()
{
#EXCLUDE_RE="(.*log4j.*)|(.*asm.*)|(.*guava.*)|(.*gson.*)"
#addToClasspath "$ACCUMULO_HOME/lib" $EXCLUDE_RE

# or use the hadoop classpath as specified by your path's hadoop command
# TODO add excludes for jars that should not be included
HADOOP_CP=$(hadoop classpath)

# use the 
TWILL_CP=lib/*:$HADOOP_CP
}

# Select which method to use for resolving Accumulo, Hadoop, and Zookeeper libraries and configuration.
loadLocalAHZLibraries
#loadSystemAHZLibraries
