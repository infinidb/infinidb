@echo off
pushd .
set help=
if "%1" == "-h" (set help=true)
if "%1" == "/h" (set help=true)
if "%1" == "--help" (set help=true)
if "%help%"=="true" (
  echo The InfiniDB Support Report creates a report that can be sent to InfiniDB
  echo for help with field support. There are no options to this command.
  set help=
  exit /B 0 
)

echo.  
echo Running the InfiniDB Support Report, outputting to InfiniDBSupportReport.txt

call :func > InfiniDBSupportReport.txt 2>&1

echo.  
echo Report finished

popd
exit /B 0

:ErrorExit

echo.  
echo Error - Failed to find InfiniDB Install Directory in Windows Registry, Exiting
popd
exit /B 1

:func

  setlocal
  set key="HKLM\SOFTWARE\Calpont\InfiniDB"
  set homeValue=InfiniDBHome
  set configValue=ConfigFile

  for /f "tokens=3,*" %%a in ('reg query %key% /ve 2^>NUL ^| findstr REG_SZ') do (
    set InfiniDBInstall=%%b
   )

  if "%InfiniDBInstall%" == "" (
  	for /f "tokens=2,*" %%a in ('reg query %key% /ve 2^>NUL ^| findstr REG_SZ') do (
    	set InfiniDBInstall=%%b
   	)
  )

  ::error out if can't locate Install Directory
  if "%InfiniDBInstall%" == "" GOTO ErrorExit

echo #######################################################################
echo #                                                                     #
echo #     InfiniDB Support Report - %date% %time%
echo #                                                                     #
echo #######################################################################
echo.
echo.
echo =======================================================================
echo =                    Software/Version Report                          =
echo =======================================================================
echo. 
echo.
echo -- InfiniDB Software Version --
type %InfiniDBInstall%\etc\CalpontVersion.txt
echo. 
echo -- mysql Software Version --
mysql --user=root -e status
echo. 
echo -- Windows Version --
ver
echo.
echo.
echo =======================================================================
echo =                    Status Report                                    =
echo =======================================================================
echo.  
echo. 
echo -- InfiniDB Process Status -- 
echo. 

tasklist /FI "Imagename eq mysqld.exe"
tasklist /FI "Imagename eq controllernode.exe"   
tasklist /FI "Imagename eq workernode.exe"
tasklist /FI "Imagename eq PrimProc.exe"
tasklist /FI "Imagename eq ExeMgr.exe"      
tasklist /FI "Imagename eq DDLProc.exe"           
tasklist /FI "Imagename eq DMLProc.exe"         
tasklist /FI "Imagename eq WriteEngineServer.exe"

echo. 
echo. 
echo =======================================================================
echo =                    Configuration Report                             =
echo =======================================================================
echo.
echo -- Windows InfiniDB Registry Values --
echo.

  echo InfiniDBInstall = %InfiniDBInstall%

  for /f "tokens=2,*" %%a in ('reg query %key% /v %homeValue% 2^>NUL ^| findstr %homeValue%') do (
    set InfiniDBHome=%%b
  )
  echo InfiniDBHome    = %InfiniDBHome%

  for /f "tokens=2,*" %%a in ('reg query %key% /v %configValue% 2^>NUL ^| findstr %configValue%') do (
    set ConfigFile=%%b
  )
  echo ConfigFile     = %ConfigFile%
echo.
echo.
echo -- InfiniDB System Configuration Information -- 
echo.
cd %InfiniDBInstall%\bin
for /f "delims=" %%a in ('getConfig.exe DBBC NumBlocksPct') do @echo NumBlocksPct = %%a
for /f "delims=" %%a in ('getConfig.exe HashJoin TotalUmMemory') do @echo TotalUmMemory = %%a
for /f "delims=" %%a in ('getConfig.exe VersionBuffer VersionBufferFileSize') do @echo VersionBufferFileSize = %%a
for /f "delims=" %%a in ('getConfig.exe ExtentMap FilesPerColumnPartition') do @echo FilesPerColumnPartition = %%a
for /f "delims=" %%a in ('getConfig.exe ExtentMap ExtentsPerSegmentFile') do @echo ExtentsPerSegmentFile = %%a
echo.
echo.
echo -- InfiniDB System Configuration File --
echo.
type "%ConfigFile%"
echo.  
echo.  
echo -- System Process Status -- 
echo. 
tasklist /v
echo.
echo =======================================================================
echo =                   Resource Usage Report                             =
echo =======================================================================
echo. 
echo -- System Information--
echo. 
systeminfo
echo. 
echo -- IP Configuration Information --
echo. 
ipconfig
echo.
echo  -- Disk BRM Data files --
echo.   
dir "%InfiniDBInstall%\dbrm\"
echo.   
echo  -- View Table Locks --
echo.   
cd %InfiniDBInstall%\bin\
viewtablelock.exe
echo.   
echo.    
echo   -- BRM Extent Map  --
echo.    
cd %InfiniDBInstall%\bin\
editem.exe -i
echo.
echo.
echo =======================================================================
echo =                   Log Report                                        =
echo =======================================================================
echo. 
echo -- InfiniDB Platform Logs --
echo. 
type "%InfiniDBInstall%\log\InfiniDBLog.txt"
echo. 
echo. 
echo -- InfiniDB MySQl log --
echo.
type "%InfiniDBInstall%\mysqldb\*.err" 
echo.
echo. 
echo -- InfiniDB Bulk Load Logs --
echo. 
dir "%InfiniDBInstall%\bulk\data"
echo. 
dir "%InfiniDBInstall%\bulk\log"
echo. 
dir "%InfiniDBInstall%\bulk\job"
echo.
echo -- Check for Errors in Bulk Logs --
echo.
cd "%InfiniDBInstall%\bulk\log"
findstr /spin /c:"error" *
findstr /spin /c:"failed" *
cd "%InfiniDBInstall%\bulk\job"
findstr /spin /c:"error" *
findstr /spin /c:"failed" *
echo.
echo =======================================================================
echo =                    DBMS Report                                      =
echo =======================================================================
echo.
echo -- DBMS InfiniDB Mysql Version -- 
echo.
mysql --user=root -e status
echo. 
echo -- DBMS Mysql InfiniDB System Column  -- 
echo. 
mysql --user=root -e "desc calpontsys.syscolumn"
echo. 
echo -- DBMS Mysql InfiniDB System Table  -- 
echo. 
mysql --user=root -e "desc calpontsys.systable"
echo. 
echo -- DBMS Mysql InfiniDB System Table Data -- 
echo.
mysql --user=root -e "select * from calpontsys.systable"
echo. 
echo -- DBMS Mysql InfiniDB Databases -- 
echo.
mysql --user=root -e "show databases"
echo.
echo -- DBMS Mysql InfiniDB variables -- 
echo. 
mysql --user=root -e "show variables"
echo. 
echo -- DBMS Mysql InfiniDB config file -- 
echo.
type "%InfiniDBInstall%\my.ini"
echo.
echo -- Active Queries -- 

::cd \InfiniDB\genii\oamapps\calpontSupport


