@echo off

echo "Launching Merge Tool..."

:: Using a wildcard in the jar filename may not work in some Windows environments,
:: so use a hard-coded filename for the jar if necessary.
SET JAR_NAME=rya.merger-*.jar

java -Xms256m -Xmx1024M -Dlog4j.configuration="file:config/log4j.xml" -cp %JAR_NAME% mvm.rya.accumulo.mr.merge.MergeTool -conf config/configuration.xml