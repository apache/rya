@echo off

echo "Launching Copy Tool..."

:: Using a wildcard in the jar filename may not work in some Windows environments,
:: so use a hard-coded filename for the jar if necessary.
SET JAR_NAME=rya.merger-*.jar

java -Xms256m -Xmx1024M -Dlog4j.configuration="file:config/copy_tool_log4j.xml" -cp %JAR_NAME% mvm.rya.accumulo.mr.merge.CopyTool -conf config/copy_tool_configuration.xml