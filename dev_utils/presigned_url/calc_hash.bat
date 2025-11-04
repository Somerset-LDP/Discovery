@echo off
setlocal enabledelayedexpansion

REM -- Validate parameter
if "%~1"=="" (
    echo Usage: %~nx0 "C:\path\to\directory"
    exit /b 1
)
set "targetDir=%~1"

if not exist "%targetDir%" (
    echo Directory not found: %targetDir%
    exit /b 1
)

echo Computing SHA256 hashes for CSV files in "%targetDir%"
echo.

REM -- Loop through CSV files (non-recursive)
for %%f in ("%targetDir%\*.csv") do (
    set "hash="
    REM get certutil output, remove header/footer lines, capture the remaining line(s)
    for /f "tokens=* delims=" %%h in ('certutil -hashfile "%%f" SHA256 ^| findstr /v /c:"SHA256 hash of file" ^| findstr /v /c:"CertUtil:"') do (
        set "hash=%%h"
    )

    if defined hash (
        REM certutil prints spaces in the hex; remove them
        set "hash=!hash: =!"
        echo %%~nxf !hash!
    ) else (
        echo %%~nxf ERROR: could not compute hash
    )
)

echo.
echo Done.
endlocal
