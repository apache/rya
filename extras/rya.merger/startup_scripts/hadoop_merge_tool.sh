#!/bin/bash

echo "Launching Merge Tool..."

hadoop jar rya.merger-*-shaded.jar mvm.rya.accumulo.mr.merge.MergeTool -conf config/configuration.xml