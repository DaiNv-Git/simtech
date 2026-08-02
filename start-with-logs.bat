@echo off
setlocal EnableExtensions DisableDelayedExpansion
chcp 65001 >nul 2>&1
title simTech GSM SMS Tool - Logging

set "APP_DIR=%~dp0"
set "JAVA_HOME=%APP_DIR%jdk"
set "JAVAFX_LIB=%APP_DIR%javafx-lib"
set "APP_JAR=%APP_DIR%simtech.jar"
set "LOG_DIR=%APP_DIR%logs"

if not exist "%LOG_DIR%" mkdir "%LOG_DIR%"

for /f %%I in ('powershell.exe -NoProfile -Command "Get-Date -Format yyyyMMdd_HHmmss"') do set "LOG_STAMP=%%I"
if not defined LOG_STAMP set "LOG_STAMP=unknown_%RANDOM%"
set "LOG_FILE=%LOG_DIR%\simtech_%LOG_STAMP%.log"

echo ============================================
echo   simTech GSM SMS Tool - Starting with logs
echo ============================================
echo Log file: "%LOG_FILE%"
echo.

echo [%date% %time%] Starting simTech > "%LOG_FILE%"
echo APP_DIR=%APP_DIR% >> "%LOG_FILE%"

if not exist "%JAVA_HOME%\bin\java.exe" (
    echo [ERROR] JDK not found at: "%JAVA_HOME%" >> "%LOG_FILE%"
    echo [ERROR] Không tìm thấy JDK đi kèm: "%JAVA_HOME%"
    echo Chi tiết: "%LOG_FILE%"
    pause
    exit /b 1
)

if not exist "%APP_JAR%" (
    echo [ERROR] Application JAR not found at: "%APP_JAR%" >> "%LOG_FILE%"
    echo [ERROR] Không tìm thấy file: "%APP_JAR%"
    echo Chi tiết: "%LOG_FILE%"
    pause
    exit /b 1
)

cd /d "%APP_DIR%"
"%JAVA_HOME%\bin\java.exe" ^
  --module-path "%JAVAFX_LIB%" ^
  --add-modules javafx.controls,javafx.web,javafx.graphics,javafx.base,javafx.media ^
  -jar "%APP_JAR%" >> "%LOG_FILE%" 2>&1

set "APP_EXIT_CODE=%ERRORLEVEL%"
echo [%date% %time%] simTech stopped with exit code %APP_EXIT_CODE% >> "%LOG_FILE%"

echo.
echo simTech đã dừng với mã: %APP_EXIT_CODE%
echo Log đã lưu tại: "%LOG_FILE%"
if not "%APP_EXIT_CODE%"=="0" (
    echo Vui lòng gửi file log này để kiểm tra lỗi.
)
pause
exit /b %APP_EXIT_CODE%
