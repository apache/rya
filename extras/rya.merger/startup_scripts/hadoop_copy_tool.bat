@echo off

echo "Launching Copy Tool..."

:: Using a wildcard in the jar filename may not work in some Windows environments,
:: so use a hard-coded filename for the jar if necessary.
SET JAR_NAME=rya.merger-*-shaded.jar

hadoop jar %JAR_NAME% mvm.rya.accumulo.mr.merge.CopyTool -conf config/copy_tool_configuration.xml