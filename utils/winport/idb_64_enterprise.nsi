!include "EnvVarUpdate.nsh"
!include "WordFunc.nsh"
!include nsDialogs.nsh
Name InfiniDB
InstallDir c:\Calpont
InstallDirRegKey HKLM SOFTWARE\Calpont\InfiniDB ""
OutFile InfiniDB64-ent.exe

!define DISPLAY_URL http://www.calpont.com/

!define DISPLAY_VERSION "4.0.0-1"
VIAddVersionKey "FileVersion" "4.0.0-1"
VIProductVersion "4.0.0.1"

VIAddVersionKey "ProductVersion" "${DISPLAY_VERSION}"
VIAddVersionKey "CompanyName" "Calpont Corp."
VIAddVersionKey "FileDescription" "Calpont InfiniDB Enterprise Windows 64-bit Installer"
VIAddVersionKey "LegalCopyright" "Copyright (c) 2010-2013"
VIAddVersionKey "ProductName" "InfiniDB"

XPStyle on

Var Dialog
Var PortLabel
Var Port
Var PortChoice
Var SvcMode
Var SvcModeChoice
Var ApndSysPath
Var ApndSysPathChoice

Section

ExecWait 'sc.exe stop InfiniDB'
IfFileExists $INSTDIR\bin\svcwait.bat 0 PreInstStopped1
ClearErrors
ExecWait '"$INSTDIR\bin\svcwait.bat" STOPPED 120'
IfErrors PreInstStopped1 0 
Delete $INSTDIR\tmp\InfiniDB.pids
PreInstStopped1:

IfFileExists $INSTDIR\bin\winfinidb.exe 0 PreInstStopped2
ExecWait '"$INSTDIR\bin\winfinidb.exe" stop'
Delete $INSTDIR\tmp\InfiniDB.pids
PreInstStopped2:

SetOutPath $INSTDIR
WriteUninstaller $INSTDIR\uninstall.exe
IfFileExists $INSTDIR\my.ini 0 MyIniNotExists
File /oname=$INSTDIR\my_dist.ini C:\InfiniDB\genii\\utils\winport\my.ini
Goto MyIniExists
MyIniNotExists:
File C:\InfiniDB\genii\\utils\winport\my.ini
MyIniExists:
SetOutPath $INSTDIR\bin

# Enterprise Release files
File C:\InfiniDB\x64\EnterpriseRelease\databaseSizeReport.exe
File C:\InfiniDB\x64\EnterpriseRelease\dumpcol.exe
File C:\InfiniDB\x64\EnterpriseRelease\oid2file.exe
File C:\InfiniDB\x64\EnterpriseRelease\dumpVss.exe
File C:\InfiniDB\infinidb-ent\tools\calpontSupport\calpontSupport.bat

# Standard Release Files
File C:\InfiniDB\x64\EnterpriseRelease\clearShm.exe
File C:\InfiniDB\x64\EnterpriseRelease\colxml.exe
File C:\InfiniDB\x64\EnterpriseRelease\controllernode.exe
File C:\InfiniDB\x64\EnterpriseRelease\cpimport.exe
File C:\InfiniDB\x64\EnterpriseRelease\dbbuilder.exe
File C:\InfiniDB\x64\EnterpriseRelease\DDLProc.exe
File C:\InfiniDB\x64\EnterpriseRelease\DMLProc.exe
File C:\InfiniDB\x64\EnterpriseRelease\editem.exe
File C:\InfiniDB\x64\EnterpriseRelease\ExeMgr.exe
File C:\InfiniDB\x64\EnterpriseRelease\load_brm.exe
File C:\InfiniDB\x64\EnterpriseRelease\WriteEngineServer.exe
File C:\InfiniDB\x64\EnterpriseRelease\DecomSvr.exe
File C:\InfiniDB\x64\EnterpriseRelease\PrimProc.exe
File C:\InfiniDB\x64\EnterpriseRelease\save_brm.exe
File C:\InfiniDB\x64\EnterpriseRelease\viewtablelock.exe
File C:\InfiniDB\x64\EnterpriseRelease\winfinidb.exe
File C:\InfiniDB\x64\EnterpriseRelease\workernode.exe
File C:\InfiniDB\x64\EnterpriseRelease\cleartablelock.exe
File C:\InfiniDB\x64\EnterpriseRelease\ddlcleanup.exe
File C:\InfiniDB\x64\EnterpriseRelease\getConfig.exe
File C:\InfiniDB\x64\EnterpriseRelease\setConfig.exe
File C:\InfiniDB\x64\EnterpriseRelease\dbrmctl.exe
File C:\InfiniDB\x64\EnterpriseRelease\reset_locks.exe
File C:\InfiniDB\mysql\sql\Release\mysqld.exe
File C:\InfiniDB\mysql\storage\myisam\Release\myisam_ftdump.exe
File C:\InfiniDB\mysql\storage\myisam\Release\myisamchk.exe
File C:\InfiniDB\mysql\storage\myisam\Release\myisamlog.exe
File C:\InfiniDB\mysql\storage\myisam\Release\myisampack.exe
File C:\InfiniDB\mysql\client\Release\mysql.exe
File C:\InfiniDB\mysql\client\Release\mysql_upgrade.exe
File C:\InfiniDB\mysql\client\Release\mysqladmin.exe
File C:\InfiniDB\mysql\client\Release\mysqlbinlog.exe
File C:\InfiniDB\mysql\client\Release\mysqlcheck.exe
File C:\InfiniDB\mysql\client\Release\mysqldump.exe
File C:\InfiniDB\mysql\client\Release\mysqlimport.exe
File C:\InfiniDB\mysql\server-tools\instance-manager\Release\mysqlmanager.exe
File C:\InfiniDB\mysql\client\Release\mysqlshow.exe
File C:\InfiniDB\mysql\client\Release\mysqlslap.exe
File C:\InfiniDB\mysql\client\Release\mysqltest.exe
File C:\InfiniDB\mysql\storage\archive\Release\ha_archive.dll
File C:\InfiniDB\mysql\storage\federated\Release\ha_federated.dll
File C:\InfiniDB\mysql\storage\innodb_plugin\Release\ha_innodb_plugin.dll
File C:\InfiniDB\mysql\libmysql\Release\libmysql.dll

File C:\InfiniDB\x64\EnterpriseRelease\libcalmysql.dll
File C:\InfiniDB\x64\EnterpriseRelease\libconfigcpp.dll
File C:\InfiniDB\x64\EnterpriseRelease\libddlpackageproc.dll
File C:\InfiniDB\x64\EnterpriseRelease\libdmlpackageproc.dll
File C:\InfiniDB\x64\EnterpriseRelease\libjoblist.dll
File C:\InfiniDB\x64\EnterpriseRelease\libwriteengine.dll
File C:\InfiniDB\x64\EnterpriseRelease\libudfsdk.dll
File C:\InfiniDB\x64\EnterpriseRelease\libudf_mysql.dll
File C:\InfiniDB\libxml2-2.7.6\libxml2.dll

File C:\InfiniDB\vcredist_x64.exe

File C:\InfiniDB\x64\EnterpriseRelease\bootstrap.exe
File C:\InfiniDB\genii\\utils\winport\svcwait.bat
File C:\InfiniDB\genii\\utils\winport\idbsvsta.bat
File C:\InfiniDB\genii\\utils\winport\idbsvsto.bat
File C:\InfiniDB\genii\\utils\winport\idbmysql.bat

SetOutPath $INSTDIR\bulk\data\import
SetOutPath $INSTDIR\bulk\job
SetOutPath $INSTDIR\bulk\log
SetOutPath $INSTDIR\data1
SetOutPath $INSTDIR\dbrm
SetOutPath $INSTDIR\etc
File C:\InfiniDB\genii\\utils\winport\win_setup_mysql_part1.sql
File C:\InfiniDB\genii\\utils\winport\win_setup_mysql_part2.sql
File C:\InfiniDB\genii\\utils\winport\win_setup_mysql_part3.sql
File C:\InfiniDB\genii\\utils\winport\win_setup_mysql_part3.1.sql
File C:\InfiniDB\genii\\utils\winport\win_setup_mysql_part4.sql
File C:\InfiniDB\genii\\utils\winport\win_setup_mysql_part5.sql
File C:\InfiniDB\genii\\utils\winport\CalpontVersion.txt
IfFileExists $INSTDIR\etc\Calpont.xml 0 CfgNotExists
File /oname=$INSTDIR\etc\Calpont_dist.xml C:\InfiniDB\genii\\utils\winport\Calpont.xml
Goto CfgExists
CfgNotExists:
File C:\InfiniDB\genii\\utils\winport\Calpont.xml
CfgExists:
File C:\InfiniDB\genii\\utils\loggingcpp\ErrorMessage.txt
File C:\InfiniDB\genii\\utils\loggingcpp\MessageFile.txt
SetOutPath $INSTDIR\log
SetOutPath $INSTDIR\local
SetOutPath $INSTDIR\mysqldb
SetOutPath $INSTDIR\share
File /r C:\InfiniDB\mysql\sql\share\charsets
File /r C:\InfiniDB\mysql\sql\share\czech
File /r C:\InfiniDB\mysql\sql\share\danish
File /r C:\InfiniDB\mysql\sql\share\dutch
File /r C:\InfiniDB\mysql\sql\share\english
File /r C:\InfiniDB\mysql\sql\share\estonian
File /r C:\InfiniDB\mysql\sql\share\french
File /r C:\InfiniDB\mysql\sql\share\german
File /r C:\InfiniDB\mysql\sql\share\greek
File /r C:\InfiniDB\mysql\sql\share\hungarian
File /r C:\InfiniDB\mysql\sql\share\italian
File /r C:\InfiniDB\mysql\sql\share\japanese
File /r C:\InfiniDB\mysql\sql\share\japanese-sjis
File /r C:\InfiniDB\mysql\sql\share\korean
File /r C:\InfiniDB\mysql\sql\share\norwegian
File /r C:\InfiniDB\mysql\sql\share\norwegian-ny
File /r C:\InfiniDB\mysql\sql\share\polish
File /r C:\InfiniDB\mysql\sql\share\portuguese
File /r C:\InfiniDB\mysql\sql\share\romanian
File /r C:\InfiniDB\mysql\sql\share\russian
File /r C:\InfiniDB\mysql\sql\share\serbian
File /r C:\InfiniDB\mysql\sql\share\slovak
File /r C:\InfiniDB\mysql\sql\share\spanish
File /r C:\InfiniDB\mysql\sql\share\swedish
File /r C:\InfiniDB\mysql\sql\share\ukrainian
SetOutPath $INSTDIR\tmp
SetOutPath $INSTDIR\sql
File C:\InfiniDB\genii\\dbcon\mysql\dumpcat_mysql.sql
File C:\InfiniDB\genii\\dbcon\mysql\calsetuserpriority.sql
File C:\InfiniDB\genii\\dbcon\mysql\calremoveuserpriority.sql
File C:\InfiniDB\genii\\dbcon\mysql\calshowprocesslist.sql

WriteRegStr HKLM Software\Calpont\InfiniDB "" $INSTDIR
WriteRegStr HKLM Software\Calpont\InfiniDB "CalpontHome" $INSTDIR\etc
WriteRegStr HKLM Software\Calpont\InfiniDB "ConfigFile" $INSTDIR\etc\Calpont.xml

StrCmp $ApndSysPathChoice 'yes' 0 DontAppendSysPath
Push "PATH"
Push "A"
Push "HKLM"
Push "$INSTDIR\bin"
Call EnvVarUpdate
Pop $0
DontAppendSysPath:

WriteRegStr HKLM "Software\Microsoft\Windows\CurrentVersion\Uninstall\InfiniDB" "DisplayName" "InfiniDB"
WriteRegStr HKLM "Software\Microsoft\Windows\CurrentVersion\Uninstall\InfiniDB" "UninstallString" "$INSTDIR\uninstall.exe"
WriteRegStr HKLM "Software\Microsoft\Windows\CurrentVersion\Uninstall\InfiniDB" "InstallLocation" "$INSTDIR"
WriteRegStr HKLM "Software\Microsoft\Windows\CurrentVersion\Uninstall\InfiniDB" "Publisher" "Calpont Corp."
WriteRegStr HKLM "Software\Microsoft\Windows\CurrentVersion\Uninstall\InfiniDB" "HelpLink" "${DISPLAY_URL}"
WriteRegStr HKLM "Software\Microsoft\Windows\CurrentVersion\Uninstall\InfiniDB" "URLUpdateInfo" "${DISPLAY_URL}"
WriteRegStr HKLM "Software\Microsoft\Windows\CurrentVersion\Uninstall\InfiniDB" "URLInfoAbout" "${DISPLAY_URL}"
WriteRegStr HKLM "Software\Microsoft\Windows\CurrentVersion\Uninstall\InfiniDB" "DisplayVersion" "${DISPLAY_VERSION}"
WriteRegDWORD HKLM "Software\Microsoft\Windows\CurrentVersion\Uninstall\InfiniDB" "VersionMajor" "1"
WriteRegDWORD HKLM "Software\Microsoft\Windows\CurrentVersion\Uninstall\InfiniDB" "VersionMinor" "1"
WriteRegDWORD HKLM "Software\Microsoft\Windows\CurrentVersion\Uninstall\InfiniDB" "NoModify" "1"
WriteRegDWORD HKLM "Software\Microsoft\Windows\CurrentVersion\Uninstall\InfiniDB" "NoRepair" "1"

ExecWait "$INSTDIR\bin\vcredist_x64.exe /q"
ClearErrors

ExecWait "$INSTDIR\bin\bootstrap.exe $PortChoice"
IfErrors 0 BootstrapOK
MessageBox MB_ICONSTOP "Fatal error installing InfiniDB"
Abort
BootstrapOK:

ExecWait '"$INSTDIR\bin\winfinidb.exe" stop'
ClearErrors

ExecWait 'sc.exe create InfiniDB binPath= "$INSTDIR\bin\winfinidb.exe" start= $SvcModeChoice'
ExecWait 'sc.exe description InfiniDB "InfiniDB Database Engine"'
StrCmp $SvcModeChoice 'auto' 0 DontStartSvc
ExecWait 'sc.exe start InfiniDB'
ExecWait '"$INSTDIR\bin\svcwait.bat" RUNNING 120'
DontStartSvc:
ClearErrors

CreateDirectory "$SMPROGRAMS\InfiniDB"
CreateShortCut "$SMPROGRAMS\InfiniDB\SQL Prompt.lnk" "$INSTDIR\bin\mysql.exe" '--defaults-file="$INSTDIR\my.ini" --user=root'
CreateShortCut "$SMPROGRAMS\InfiniDB\Start InfiniDB.lnk" "$INSTDIR\bin\idbsvsta.bat"
CreateShortCut "$SMPROGRAMS\InfiniDB\Stop InfiniDB.lnk" "$INSTDIR\bin\idbsvsto.bat"
WriteINIStr "$SMPROGRAMS\InfiniDB\InfiniDB on the Web.url" "InternetShortcut" "URL" "${DISPLAY_URL}"
ClearErrors

SectionEnd

Section Uninstall
SetRegView 64
ExecWait 'sc.exe stop InfiniDB'
ClearErrors
ExecWait '"$INSTDIR\bin\svcwait.bat" STOPPED 120'
IfErrors PreUninstStopped1 0 
Delete $INSTDIR\tmp\InfiniDB.pids
PreUninstStopped1:
ExecWait '"$INSTDIR\bin\winfinidb.exe" stop'
Delete $INSTDIR\tmp\InfiniDB.pids
ExecWait 'sc.exe delete InfiniDB'

Push "PATH"
Push "R"
Push "HKLM"
Push "$INSTDIR\bin"
Call un.EnvVarUpdate
Pop $0

Delete $INSTDIR\uninstall.exe

# Enterprise Release Files
Delete $INSTDIR\bin\databaseSizeReport.exe
Delete $INSTDIR\bin\dumpcol.exe
Delete $INSTDIR\bin\oid2file.exe
Delete $INSTDIR\bin\dumpVss.exe
Delete $INSTDIR\bin\calpontSupport.bat

# Standard Release Files
Delete $INSTDIR\bin\bootstrap.exe
Delete $INSTDIR\bin\clearShm.exe
Delete $INSTDIR\bin\colxml.exe
Delete $INSTDIR\bin\controllernode.exe
Delete $INSTDIR\bin\cpimport.exe
Delete $INSTDIR\bin\dbbuilder.exe
Delete $INSTDIR\bin\DDLProc.exe
Delete $INSTDIR\bin\DMLProc.exe
Delete $INSTDIR\bin\editem.exe
Delete $INSTDIR\bin\ExeMgr.exe
Delete $INSTDIR\bin\libcalmysql.dll
Delete $INSTDIR\bin\libconfigcpp.dll
Delete $INSTDIR\bin\libddlpackageproc.dll
Delete $INSTDIR\bin\libdmlpackageproc.dll
Delete $INSTDIR\bin\libjoblist.dll
Delete $INSTDIR\bin\libwriteengine.dll
Delete $INSTDIR\bin\libudfsdk.dll
Delete $INSTDIR\bin\libudf_mysql.dll
Delete $INSTDIR\bin\libxml2.dll
Delete $INSTDIR\bin\load_brm.exe
Delete $INSTDIR\bin\myisam_ftdump.exe
Delete $INSTDIR\bin\myisamchk.exe
Delete $INSTDIR\bin\myisamlog.exe
Delete $INSTDIR\bin\myisampack.exe
Delete $INSTDIR\bin\mysql.exe
Delete $INSTDIR\bin\mysql_upgrade.exe
Delete $INSTDIR\bin\mysqladmin.exe
Delete $INSTDIR\bin\mysqlbinlog.exe
Delete $INSTDIR\bin\mysqlcheck.exe
Delete $INSTDIR\bin\mysqld.exe
Delete $INSTDIR\bin\mysqldump.exe
Delete $INSTDIR\bin\mysqlimport.exe
Delete $INSTDIR\bin\mysqlmanager.exe
Delete $INSTDIR\bin\mysqlshow.exe
Delete $INSTDIR\bin\mysqlslap.exe
Delete $INSTDIR\bin\mysqltest.exe
Delete $INSTDIR\bin\DecomSvr.exe
Delete $INSTDIR\bin\PrimProc.exe
Delete $INSTDIR\bin\WriteEngineServer.exe
Delete $INSTDIR\bin\save_brm.exe
Delete $INSTDIR\bin\vcredist_x64.exe
Delete $INSTDIR\bin\viewtablelock.exe
Delete $INSTDIR\bin\winfinidb.exe
Delete $INSTDIR\bin\workernode.exe
Delete $INSTDIR\bin\cleartablelock.exe
Delete $INSTDIR\bin\ddlcleanup.exe
Delete $INSTDIR\bin\getConfig.exe
Delete $INSTDIR\bin\setConfig.exe
Delete $INSTDIR\bin\dbrmctl.exe
Delete $INSTDIR\bin\reset_locks.exe
Delete $INSTDIR\bin\svcwait.bat
Delete $INSTDIR\bin\idbsvsta.bat
Delete $INSTDIR\bin\idbsvsto.bat
Delete $INSTDIR\bin\idbmysql.bat
Delete $INSTDIR\bin\ha_archive.dll
Delete $INSTDIR\bin\ha_federated.dll
Delete $INSTDIR\bin\ha_innodb_plugin.dll
Delete $INSTDIR\bin\libmysql.dll

Delete $INSTDIR\etc\ErrorMessage.txt
Delete $INSTDIR\etc\MessageFile.txt
Delete $INSTDIR\etc\win_setup_mysql_part1.sql
Delete $INSTDIR\etc\win_setup_mysql_part2.sql
Delete $INSTDIR\etc\win_setup_mysql_part3.sql
Delete $INSTDIR\etc\win_setup_mysql_part3.1.sql
Delete $INSTDIR\etc\win_setup_mysql_part4.sql
Delete $INSTDIR\etc\win_setup_mysql_part5.sql
Delete $INSTDIR\etc\Calpont_save.xml
Rename $INSTDIR\etc\Calpont.xml $INSTDIR\etc\Calpont_save.xml

RMDir /r $INSTDIR\share\charsets
RMDir /r $INSTDIR\share\czech
RMDir /r $INSTDIR\share\danish
RMDir /r $INSTDIR\share\dutch
RMDir /r $INSTDIR\share\english
RMDir /r $INSTDIR\share\estonian
RMDir /r $INSTDIR\share\french
RMDir /r $INSTDIR\share\german
RMDir /r $INSTDIR\share\greek
RMDir /r $INSTDIR\share\hungarian
RMDir /r $INSTDIR\share\italian
RMDir /r $INSTDIR\share\japanese
RMDir /r $INSTDIR\share\japanese-sjis
RMDir /r $INSTDIR\share\korean
RMDir /r $INSTDIR\share\norwegian
RMDir /r $INSTDIR\share\norwegian-ny
RMDir /r $INSTDIR\share\polish
RMDir /r $INSTDIR\share\portuguese
RMDir /r $INSTDIR\share\romanian
RMDir /r $INSTDIR\share\russian
RMDir /r $INSTDIR\share\serbian
RMDir /r $INSTDIR\share\slovak
RMDir /r $INSTDIR\share\spanish
RMDir /r $INSTDIR\share\swedish
RMDir /r $INSTDIR\share\ukrainian

Delete $INSTDIR\sql\dumpcat_mysql.sql
Delete $INSTDIR\sql\calsetuserpriority.sql
Delete $INSTDIR\sql\calremoveuserpriority.sql
Delete $INSTDIR\sql\calshowprocesslist.sql

Delete $INSTDIR\my_save.ini
Rename $INSTDIR\my.ini $INSTDIR\my_save.ini

RMDir /r "$SMPROGRAMS\InfiniDB"

DeleteRegKey HKLM "Software\Microsoft\Windows\CurrentVersion\Uninstall\InfiniDB"

SectionEnd

Function .onInit
SetRegView 64
ClearErrors
userInfo::getAccountType
pop $0
StrCmp $0 "Admin" AdminOK
MessageBox MB_ICONSTOP "Administrator privileges are required to install InfiniDB"
Abort
AdminOK:
#set up defaults for these things we ask the user about.
StrCpy $PortChoice "3306"
StrCpy $SvcModeChoice "auto"
StrCpy $ApndSysPathChoice "yes"
# We need to run the 32 bit version of NSIS until the 8K string patch is available for
# the 64 bit release.
# Since we're running the 32 bit version, InstallDirRegKey looks in the wrong place
# because Windows 64 silently puts all the 32 bit registry stuff in Wow6432Node, which
# is where InstallDirRegKey looks. But we don't put our stuff there.
# For some reason, NSIS won't let SetRegView (which would fix the problem) run until after
# InstallDirRegKey is run. Hence this hack to get our install directory from the registry
SetRegView 64
ClearErrors
ReadRegStr $0 HKLM Software\Calpont\InfiniDB ""
IfErrors GotInstDir
StrCpy $INSTDIR $0
GotInstDir:
FunctionEnd

Function nsDialogsPage
	nsDialogs::Create 1018
	Pop $Dialog

	${NSD_CreateLabel} 0 25u 75u 12u "mysqld port number: "
	Pop $PortLabel

	${NSD_CreateNumber} 75u 25u 50u 12u "3306"
	Pop $Port

	${NSD_CreateCheckBox} 0 50u 100% 12u " Start InfiniDB automatically"
	Pop $SvcMode

	${NSD_SetState} $SvcMode ${BST_CHECKED}

	${NSD_CreateCheckBox} 0 75u 100% 12u " Append InfiniDB to system PATH"
	Pop $ApndSysPath

	${NSD_SetState} $ApndSysPath ${BST_CHECKED}

	${NSD_CreateLabel} 12u 88u 100% 12u "CAUTION: if your system PATH is at or near 8K chars in length"
	${NSD_CreateLabel} 12u 98u 100% 12u "uncheck this box and set the system PATH manually!"

	nsDialogs::Show

FunctionEnd

Function nsDialogsPageLeave

	${NSD_GetText} $Port $PortChoice
	${NSD_GetState} $SvcMode $9
	StrCmp $9 ${BST_UNCHECKED} 0 ModeChecked
	StrCpy $SvcModeChoice "demand"
ModeChecked:
	${NSD_GetState} $ApndSysPath $9
	StrCmp $9 ${BST_UNCHECKED} 0 AppendChecked
	StrCpy $ApndSysPathChoice "no"
AppendChecked:

FunctionEnd

Page directory
Page custom nsDialogsPage nsDialogsPageLeave
Page instfiles

