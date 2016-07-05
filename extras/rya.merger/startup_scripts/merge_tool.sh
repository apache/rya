#!/bin/bash

echo "Launching Merge Tool..."

java -Xms256m -Xmx1024M -Dlog4j.configuration="file:config/log4j.xml" -cp rya.merger-*.jar mvm.rya.accumulo.mr.merge.MergeTool -conf config/configuration.xml