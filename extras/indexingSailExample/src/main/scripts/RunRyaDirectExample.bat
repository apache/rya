@echo off
SET CP=

REM Check to see if javac is on the path
where /Q javac
IF %ERRORLEVEL% NEQ 0 goto :NO_JAVAC


for /f %%f in ('DIR /b .\lib\*.jar') do call :append .\lib\%%f

javac -cp "%CP%" RyaDirectExample.java
java -cp "%CP%" RyaDirectExample

goto :end

:append
@echo off
SET CP=%CP%%1;
goto :end

:NO_JAVAC
echo ERROR: Could not find javac
goto :end

:end