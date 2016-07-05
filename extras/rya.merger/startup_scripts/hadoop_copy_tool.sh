#!/bin/bash

echo "Launching Copy Tool..."

class=mvm.rya.accumulo.mr.merge.CopyTool

command="${ACCUMULO_HOME}/bin/tool.sh"
if [ ! -x $command ]; then
    export HADOOP_CLASSPATH="${HADOOP_CLASSPATH}:lib/*"
    command="hadoop jar"
fi

$command rya.merger-*-shaded.jar mvm.rya.accumulo.mr.merge.CopyTool -conf config/copy_tool_configuration.xml
