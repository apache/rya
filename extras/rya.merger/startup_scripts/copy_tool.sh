#!/bin/bash

echo "Launching Copy Tool..."

java -Xms256m -Xmx1024M -Dlog4j.configuration="file:config/copy_tool_log4j.xml" -cp rya.merger-*.jar mvm.rya.accumulo.mr.merge.CopyTool -conf config/copy_tool_configuration.xml