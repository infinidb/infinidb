@echo off

echo ======================================
echo updating mysql source
cd \InfiniDB\mysql
git checkout develop
git pull
IF %ERRORLEVEL% NEQ 0 GOTO ERROR_HANDLER

echo ======================================
echo updating InfiniDB source
cd \InfiniDB\genii
git checkout develop
git pull origin_http develop
IF %ERRORLEVEL% NEQ 0 GOTO ERROR_HANDLER

echo ======================================
echo Building mysql
cd \InfiniDB\mysql
MSBuild /M:8 /t:rebuild /p:Configuration="Release" /p:Platform="x64" mysql.sln
IF %ERRORLEVEL% NEQ 0 GOTO ERROR_HANDLER

echo ======================================
echo Building InfiniDB
cd \InfiniDB\genii\build
MSBuild /M:8 /t:rebuild /p:Configuration="EnterpriseRelease" /p:Platform="x64" InfiniDB.sln
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

cd \InfiniDB\genii\build
exit /B 1

:QUIT
echo ======================================
cd \InfiniDB\genii\build
echo    compiled at %date% %time%
echo nightly build complete

