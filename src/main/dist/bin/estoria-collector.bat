@echo off

setlocal ENABLEDELAYEDEXPANSION

REM Resolve script directory
set SCRIPT_DIR=%~dp0
REM Remove trailing backslash if present
if "%SCRIPT_DIR:~-1%"=="\" set SCRIPT_DIR=%SCRIPT_DIR:~0,-1%

REM Assume layout: ROOT\bin, ROOT\lib, ROOT\conf
for %%I in ("%SCRIPT_DIR%") do set ROOT_DIR=%%~dpI
if "%ROOT_DIR:~-1%"=="\" set ROOT_DIR=%ROOT_DIR:~0,-1%

set LIB_DIR=%ROOT_DIR%\lib
set CONF_DIR=%ROOT_DIR%\conf
set PLUGINS_DIR=%ROOT_DIR%\plugins
set CLASSPATH=%CONF_DIR%;%LIB_DIR%\*;%PLUGINS_DIR%\*
set MAIN_CLASS=io.coherity.estoria.collector.engine.impl.cli.Main

java %JAVA_OPTS% -cp "%CLASSPATH%" %MAIN_CLASS% %*

endlocal
