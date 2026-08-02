@echo off
chcp 65001 >nul 2>&1
title Create SimSmart GSM Shortcut

set "SCRIPT_DIR=%~dp0"
set "EXE_PATH=%SCRIPT_DIR%SimSmartGSM.exe"
set "VBS_SCRIPT=%TEMP%\CreateShortcut_%RANDOM%.vbs"

echo ============================================
echo   Tạo Shortcut ngoài màn hình Desktop cho
echo   SimSmart GSM...
echo ============================================

REM Tạo script VBS để tự động tạo Shortcut
echo Set oWS = WScript.CreateObject("WScript.Shell") > "%VBS_SCRIPT%"
echo sLinkFile = oWS.SpecialFolders("Desktop") ^& "\SimSmart GSM.lnk" >> "%VBS_SCRIPT%"
echo Set oLink = oWS.CreateShortcut(sLinkFile) >> "%VBS_SCRIPT%"
echo oLink.TargetPath = "%EXE_PATH%" >> "%VBS_SCRIPT%"
echo oLink.WorkingDirectory = "%SCRIPT_DIR%" >> "%VBS_SCRIPT%"
echo oLink.Description = "Phần mềm SimSmart GSM" >> "%VBS_SCRIPT%"
echo oLink.Save >> "%VBS_SCRIPT%"

REM Chạy đoạn script VBS
cscript /nologo "%VBS_SCRIPT%"
del "%VBS_SCRIPT%"

echo.
echo Thanh cong! Ban co the ra ngoai Desktop de kiem tra.
echo ============================================
pause
