:: This nightly script is customized for srvbldwin1
:: It probably needs to be modified if used for other machines
:: Nightly.bat needs be moved to c:\ and run from there.

::@echo off

:: If there's a command line branch, use it.
set branch=%1
IF "%1" == "" (
:: No command line, use file
  FOR /F %%i IN (branch.txt) DO set branch=%%i
)
echo %branch%
IF "%branch%" == "trunk" (
  set basedir=\InfiniDB
) ELSE (
 set basedir=\InfiniDB_%branch%
)

:: Setup branch.txt for next run
:: default is to run trunk next
set nextrun=trunk
echo %nextrun%
IF "%branch%" == "trunk" set nextrun=4.0
echo %nextrun%
IF "%branch%" == "4.0" set nextrun=4.5
echo %nextrun%
echo %nextrun% > branch.txt

echo Building %branch% at %basedir%

:: Checkout the server
bash %basedir%\genii\tools\reserveStacks\stack reserve srvbldwin1 nightly f:/Calpont

:: create a time with a leading zero if hour < 10
set MYTIME=%TIME: =0%
set archivedirname=%DATE:~10,4%-%DATE:~4,2%-%DATE:~7,2%@%MYTIME:~0,2%.%MYTIME:~3,2%.%MYTIME:~6,2%

echo Building the application
cd %basedir%\genii\build

call Build.bat %branch%
IF %ERRORLEVEL% NEQ 0 GOTO ERROR_HANDLER

echo Running the installer
cd %basedir%\genii\utils\winport
InfiniDB64-ent.exe /S /D=f:\Calpont
IF %ERRORLEVEL% NEQ 0 GOTO ERROR_HANDLER
cd \

echo Waiting for InfiniDB service to boot
call svcwait.bat
IF %ERRORLEVEL% NEQ 0 GOTO ERROR_HANDLER
echo InfiniDB service is ready

echo copying the installer to calweb
call %basedir%\genii\build\CopyToCalweb.bat

echo running the nightly test scripts
cd %basedir%\genii\mysql\queries\nightly\srvswdev11
bash ./go.sh f:/Calpont
GOTO QUIT

:ERROR_HANDLER
echo.
echo Build error occured. Nightly tests not run >> build.log
cd \
expect %basedir%/genii/build/CopyLog.sh
xcopy build.log f:\nightly\build_log\%archivedirname%\
bash %basedir%\genii\tools\reserveStacks\stack release srvbldwin1 nightly f:/Calpont
exit 1

:QUIT
echo nightly tests complete >> go.log
echo %date% %time% >> go.log
cd \
expect %basedir%/genii/build/CopyLog.sh
xcopy build.log f:\nightly\build_log\%archivedirname%\
bash %basedir%\genii\tools\reserveStacks\stack release srvbldwin1 nightly f:/Calpont
:END