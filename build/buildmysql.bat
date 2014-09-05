echo patching mysql with Calpont modifications
call getfiles.bat
IF %ERRORLEVEL% NEQ 0 GOTO ERROR_HANDLER
cd ..\..\mysql-5.1.39
echo building mysql
VCBUILD /M2 /rebuild mysql.sln "Release|x64
cd ..\genii\build
IF %ERRORLEVEL% NEQ 0 GOTO ERROR_HANDLER\
exit 0

:ERROR_HANDLER
echo.
echo Error occured during mysql build. InfiniDB not installed
exit 1
