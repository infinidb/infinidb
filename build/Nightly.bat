:: This nightly script is customized for srvbldwin1
:: It probably needs to be modified if used for other machines
:: Note the call to Switch.bat. It needs to be changed for different
:: releases.
:: Nightly.bat and Switch.bat need be moved to c:\ and run from there.

@echo off
bash \InfiniDB\genii\tools\reserveStacks\stack reserve srvbldwin1 nightly f:/Calpont
call Switch.bat

:: create a time with a leading zero if hour < 10
set MYTIME=%TIME: =0%
set archivedirname=%DATE:~10,4%-%DATE:~4,2%-%DATE:~7,2%@%MYTIME:~0,2%.%MYTIME:~3,2%.%MYTIME:~6,2%

echo Building the application
cd \InfiniDB\genii\build
call Build.bat
IF %ERRORLEVEL% NEQ 0 GOTO ERROR_HANDLER

echo Running the installer
cd \InfiniDB\genii\utils\winport
InfiniDB64-ent.exe /S /D=f:\Calpont
IF %ERRORLEVEL% NEQ 0 GOTO ERROR_HANDLER
cd \

echo Waiting for InfiniDB service to boot
call svcwait.bat
IF %ERRORLEVEL% NEQ 0 GOTO ERROR_HANDLER
echo InfiniDB service is ready

echo copying the installer to calweb
call \InfiniDB\genii\build\CopyToCalweb.bat

echo running the nightly test scripts
cd \InfiniDB\genii\mysql\queries\nightly\srvswdev11
bash ./go.sh f:/Calpont
GOTO QUIT

:ERROR_HANDLER
echo.
echo Build error occured. Nightly tests not run >> build.log
cd \
expect /InfiniDB/genii/build/CopyLog.sh
xcopy build.log f:\nightly\build_log\%archivedirname%\
exit 1

:QUIT
echo nightly tests complete >> go.log
echo %date% %time% >> go.log
cd \
expect /InfiniDB/genii/build/CopyLog.sh
xcopy build.log f:\nightly\build_log\%archivedirname%\
bash \InfiniDB\genii\tools\reserveStacks\stack release srvbldwin1 nightly f:/Calpont

