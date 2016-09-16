@echo off
@rem/*
@rem * Licensed to the Apache Software Foundation (ASF) under one
@rem * or more contributor license agreements.  See the NOTICE file
@rem * distributed with this work for additional information
@rem * regarding copyright ownership.  The ASF licenses this file
@rem * to you under the Apache License, Version 2.0 (the
@rem * "License"); you may not use this file except in compliance
@rem * with the License.  You may obtain a copy of the License at
@rem *
@rem *     http://www.apache.org/licenses/LICENSE-2.0
@rem *
@rem * Unless required by applicable law or agreed to in writing, software
@rem * distributed under the License is distributed on an "AS IS" BASIS,
@rem * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
@rem * See the License for the specific language governing permissions and
@rem * limitations under the License.
@rem */
@rem 
setlocal EnableExtensions EnableDelayedExpansion

rem ----
rem In order to pass in arguments with an equals symbol, use quotation marks.
rem For example
rem sqlline -u "jdbc:drill:zk=local" -n admin -p admin
rem ----

rem ----
rem Deal with command-line arguments
rem ----

:argactionstart
if -%1-==-- goto argactionend

set atleastonearg=0

if x%1 == x-q (
  set QUERY=%2
  set atleastonearg=1
  shift
  shift
)

if x%1 == x-e (
  set QUERY=%2
  set atleastonearg=1
  shift
  shift
)

if x%1 == x-f (
  set FILE=%2
  set atleastonearg=1
  shift
  shift
)

if x%1 == x--config (
  set confdir=%2
  set DRILL_CONF_DIR=%2
  set atleastonearg=1
  shift
  shift
)

if "!atleastonearg!"=="0" (
  set DRILL_ARGS=!DRILL_ARGS! %~1
  shift
)

goto argactionstart
:argactionend

rem ----
rem Validate JAVA_HOME
rem ----
if "%JAVA_EXE%" == "" (set JAVA_EXE=java.exe)

if not "%JAVA_HOME%" == "" goto javaHomeSet
echo.
echo WARN: JAVA_HOME not found in your environment.
echo Please set the JAVA_HOME variable in your environment to match the
echo location of your Java installation
echo.
goto initDrillEnv

:javaHomeSet
if exist "%JAVA_HOME%\bin\%JAVA_EXE%" goto initDrillEnv
echo.
echo ERROR: JAVA_HOME is set to an invalid directory.
echo JAVA_HOME = %JAVA_HOME%
echo Please set the JAVA_HOME variable in your environment to match the
echo location of your Java installation
echo.
goto error

:initDrillEnv
echo DRILL_ARGS - "%DRILL_ARGS%"

rem ----
rem Deal with Drill variables
rem ----

set DRILL_BIN_DIR=%~dp0
pushd %DRILL_BIN_DIR%..
set DRILL_HOME=%cd%
popd

if "test%DRILL_CONF_DIR%" == "test" (
  set DRILL_CONF_DIR=%DRILL_HOME%\conf
)

if "test%DRILL_LOG_DIR%" == "test" (
  set DRILL_LOG_DIR=%DRILL_HOME%\log
)

rem ----
rem Deal with Hadoop JARs, if HADOOP_HOME was specified
rem ----

if "test%HADOOP_HOME%" == "test" (
  echo HADOOP_HOME not detected...
  set USE_HADOOP_CP=0
  set HADOOP_HOME=%DRILL_HOME%\winutils
) else (
  echo Calculating HADOOP_CLASSPATH ...
  for %%i in (%HADOOP_HOME%\lib\*.jar) do (
    set IGNOREJAR=0
    for /F "tokens=*" %%A in (%DRILL_BIN_DIR%\hadoop-excludes.txt) do (
      echo.%%~ni|findstr /C:"%%A" >nul 2>&1
      if not errorlevel 1 set IGNOREJAR=1
    )
    if "!IGNOREJAR!"=="0" set HADOOP_CLASSPATH=%%i;!HADOOP_CLASSPATH!
  )
  set HADOOP_CLASSPATH=%HADOOP_HOME%\conf;!HADOOP_CLASSPATH!
  set USE_HADOOP_CP=1
)

rem ----
rem Deal with HBase JARs, if HBASE_HOME was specified
rem ----

if "test%HBASE_HOME%" == "test" (
  echo HBASE_HOME not detected...
  set USE_HBASE_CP=0
) else (
  echo Calculating HBASE_CLASSPATH ...
  for %%i in (%HBASE_HOME%\lib\*.jar) do (
    set IGNOREJAR=0
    for /F "tokens=*" %%A in (%DRILL_BIN_DIR%\hadoop-excludes.txt) do (
      echo.%%~ni|findstr /C:"%%A" >nul 2>&1
      if not errorlevel 1 set IGNOREJAR=1
    )
    if "!IGNOREJAR!"=="0" set HBASE_CLASSPATH=%%i;!HBASE_CLASSPATH!
  )
  set HBASE_CLASSPATH=%HADOOP_HOME%\conf;!HBASE_CLASSPATH!
  set USE_HBASE_CP=1
)

echo Calculating Drill classpath...

set DRILL_CP=%DRILL_CONF_DIR%
if NOT "test%DRILL_CLASSPATH_PREFIX%"=="test" set DRILL_CP=!DRILL_CP!;%DRILL_CLASSPATH_PREFIX%
set DRILL_CP=%DRILL_CP%;%DRILL_HOME%\jars\*
set DRILL_CP=%DRILL_CP%;%DRILL_HOME%\jars\ext\*
if "test%USE_HADOOP_CP%"=="test1" set DRILL_CP=!DRILL_CP!;%HADOOP_CLASSPATH%
if "test%USE_HBASE_CP%"=="test1" set DRILL_CP=!DRILL_CP!;%HBASE_CLASSPATH%
set DRILL_CP=%DRILL_CP%;%DRILL_HOME%\jars\3rdparty\*
set DRILL_CP=%DRILL_CP%;%DRILL_HOME%\jars\classb\*
if NOT "test%DRILL_CLASSPATH%"=="test" set DRILL_CP=!DRILL_CP!;%DRILL_CLASSPATH%

rem Override SQLLine's default initial transaction isolation level.  (SQLLine
rem sets an initial level instead of leaving it at whatever the Driver's default
rem is.)
rem Put our property specification before previous value of DRILL_SHELL_JAVA_OPTS
rem so that it can still be overridden via DRILL_SHELL_JAVA_OPTS.
rem 
rem This is not currently needed as the new SQLLine we are using doesn't isolate.
rem set DRILL_SHELL_JAVA_OPTS=-Dsqlline.isolation=TRANSACTION_NONE %DRILL_SHELL_JAVA_OPTS%

set DRILL_SHELL_JAVA_OPTS=%DRILL_SHELL_JAVA_OPTS% -Dlog.path="%DRILL_LOG_DIR%\sqlline.log" -Dlog.query.path="%DRILL_LOG_DIR%\sqlline_queries.log"

SET JAVA_CMD=%JAVA_HOME%\bin\%JAVA_EXE%
if "%JAVA_HOME%" == "" (set JAVA_CMD=%JAVA_EXE%)
set ERROR_CODE=0

if NOT "test%QUERY%"=="test" (
  echo %QUERY% | "%JAVA_CMD%" %DRILL_SHELL_JAVA_OPTS% %DRILL_JAVA_OPTS% -cp "%DRILL_CP%" sqlline.SqlLine -d org.apache.drill.jdbc.Driver %DRILL_ARGS%
) else (
  if NOT "test%FILE%"=="test" (
    "%JAVA_CMD%" %DRILL_SHELL_JAVA_OPTS% %DRILL_JAVA_OPTS% -cp "%DRILL_CP%" sqlline.SqlLine -d org.apache.drill.jdbc.Driver %DRILL_ARGS% --run=%FILE%
  ) else (
    "%JAVA_CMD%" %DRILL_SHELL_JAVA_OPTS% %DRILL_JAVA_OPTS% -cp "%DRILL_CP%" sqlline.SqlLine -d org.apache.drill.jdbc.Driver %DRILL_ARGS%
  )
)
if ERRORLEVEL 1 goto error
goto end

:error
set ERROR_CODE=1

:end
endlocal
exit /B %ERROR_CODE%
