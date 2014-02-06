::@echo off

echo ======================================
echo updating mysql source
cd \InfiniDB\mysql
git checkout 4.0
git pull
IF %ERRORLEVEL% NEQ 0 GOTO ERROR_HANDLER

echo ======================================
echo updating InfiniDB source
cd \InfiniDB\genii
git checkout 4.0
git pull origin_http 4.0
IF %ERRORLEVEL% NEQ 0 GOTO ERROR_HANDLER

echo ======================================
echo Building mysql
cd \InfiniDB\mysql
VCBUILD /M8 /rebuild mysql.sln "Release|x64"
IF %ERRORLEVEL% NEQ 0 GOTO ERROR_HANDLER

echo ======================================
echo Building InfiniDB
cd \InfiniDB\genii\build
VCBUILD /M8 /rebuild InfiniDB.sln "EnterpriseRelease|x64"
IF %ERRORLEVEL% NEQ 0 GOTO ERROR_HANDLER

echo ======================================
echo Building infindb-ent
cd \InfiniDB\infinidb-ent\build
call Build.bat
IF %ERRORLEVEL% NEQ 0 GOTO ERROR_HANDLER
cd \InfiniDB\genii\build

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
exit /B 1

:QUIT
echo ======================================
echo    compiled at %date% %time%
echo nightly build complete

