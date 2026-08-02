@echo off
chcp 65001 >nul 2>&1
title SimSmart GSM Gateway
echo ============================================
echo   SimSmart GSM Gateway - Starting...
echo ============================================
echo.

REM --- Set paths relative to this script ---
set "APP_DIR=%~dp0"
set "JAVA_HOME=%APP_DIR%jdk"
set "PATH=%JAVA_HOME%\bin;%PATH%"

REM --- Check if JDK exists ---
if not exist "%JAVA_HOME%\bin\java.exe" (
    echo [ERROR] JDK not found at: %JAVA_HOME%
    echo Please ensure the 'jdk' folder is in the same directory as this script.
    pause
    exit /b 1
)

REM --- Check if JAR exists ---
if not exist "%APP_DIR%simsmart-gsm.jar" (
    echo [ERROR] simsmart-gsm.jar not found in: %APP_DIR%
    pause
    exit /b 1
)

REM --- Create logs directory if not exist ---
if not exist "%APP_DIR%logs" mkdir "%APP_DIR%logs"

REM --- Create data directory if not exist ---
if not exist "%APP_DIR%data" mkdir "%APP_DIR%data"

echo Java Home   : %JAVA_HOME%
echo JavaFX Lib  : %APP_DIR%javafx-lib
echo.
echo Starting application...
echo.

REM --- Run the application with JavaFX module-path ---
"%JAVA_HOME%\bin\java.exe" --module-path "%APP_DIR%javafx-lib" --add-modules javafx.controls,javafx.web,javafx.graphics,javafx.base,javafx.media -jar "%APP_DIR%simsmart-gsm.jar" --spring.profiles.active=prod

echo.
echo ============================================
echo   Application stopped.
echo ============================================
pause
