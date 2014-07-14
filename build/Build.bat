::@echo off

set branch=%1
set basedir=\InfiniDB_%branch%
IF "%branch%" == "" (
  set branch=develop
  set basedir=\InfiniDB
)
IF "%branch%" == "trunk" (
  set branch=develop
  set basedir=\InfiniDB
)
echo %branch%

echo building %basedir%

echo ======================================
echo updating mysql source
cd %basedir%\mysql
git checkout %branch%
git stash
git pull
git stash pop

echo ======================================
echo updating InfiniDB source
cd %basedir%\genii
git checkout %branch%
git pull origin_http %branch%
IF %ERRORLEVEL% NEQ 0 GOTO ERROR_HANDLER

echo ======================================
echo Building mysql
cd %basedir%\mysql
MSBuild /M:8 /t:rebuild /p:Configuration="Release" /p:Platform="x64" mysql.sln
IF %ERRORLEVEL% NEQ 0 GOTO ERROR_HANDLER

echo ======================================
echo Building InfiniDB
cd %basedir%\genii\build
MSBuild /M:8 /t:rebuild /p:Configuration="EnterpriseRelease" /p:Platform="x64" InfiniDB.sln
IF %ERRORLEVEL% NEQ 0 GOTO ERROR_HANDLER

echo ======================================
echo Building infindb-ent
cd %basedir%\infinidb-ent\build
call Build.bat
IF %ERRORLEVEL% NEQ 0 GOTO ERROR_HANDLER
cd %basedir%\genii\build

echo ======================================
echo Building CalpontVersion.txt
call BuildCalpontVersion.bat

echo ======================================
echo Building the Standard Installer
makensis ..\utils\winport\idb_64_standard.nsi
IF %ERRORLEVEL% NEQ 0 GOTO ERROR_HANDLER

echo ======================================
echo Building the Enterprise Installer
makensis ..\utils\winport\idb_64_enterprise.nsi
IF %ERRORLEVEL% NEQ 0 GOTO ERROR_HANDLER
echo.

echo Build complete
GOTO QUIT

:ERROR_HANDLER
echo.
echo ======================================
echo Error occured. InfiniDB not built

cd %basedir%\genii\build
exit /B 1

:QUIT
echo ======================================
cd %basedir%\genii\build
echo    compiled at %date% %time%
echo nightly build complete

