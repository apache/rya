#!/bin/bash

echo "Launching Copy Tool..."

hadoop jar rya.merger-*-shaded.jar mvm.rya.accumulo.mr.merge.CopyTool -conf config/copy_tool_configuration.xml