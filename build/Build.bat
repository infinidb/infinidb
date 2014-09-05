::@echo off

echo updating the source
cd \InfiniDB\genii
git checkout 3.6
git pull origin_http 3.6

:: The update retrieves files that are Linux symlinks. These
:: files are useless to us and cause compile failures. For now,
:: just delete them.
cd primitives\primproc
rm rowaggregation.*
rm rowgroup.*
cd ..\..
IF %ERRORLEVEL% NEQ 0 GOTO ERROR_HANDLER

echo patching mysql with Calpont modifications
cd build
call getfiles.bat
IF %ERRORLEVEL% NEQ 0 GOTO ERROR_HANDLER
echo building mysql
cd ..\..\mysql-5.1.39
VCBUILD /M8 /rebuild mysql.sln "Release|x64"
IF %ERRORLEVEL% NEQ 0 GOTO ERROR_HANDLER

echo Building the executables
cd ..\genii\build
VCBUILD /M8 /rebuild InfiniDB.sln "EnterpriseRelease|x64"
IF %ERRORLEVEL% NEQ 0 GOTO ERROR_HANDLER

call BuildCalpontVersion.bat

echo Building the Installer
makensis ..\utils\winport\idb_64_enterprise.nsi
IF %ERRORLEVEL% NEQ 0 GOTO ERROR_HANDLER
echo.
echo Build complete
GOTO QUIT

:ERROR_HANDLER
echo.
echo Error occured. InfiniDB not built
exit 1

:QUIT
echo    compiled at %date% %time%
echo nightly build complete

