::@echo off
IF NOT EXIST C:\InfiniDB_3.6\NUL GOTO SWITCH_trunk
IF NOT EXIST C:\InfiniDB_trunk\NUL GOTO SWITCH_36
::IF NOT EXIST C:\InfiniDB_caldb\NUL GOTO SWITCH_36

:SWITCH_trunk
echo switching to trunk
REN "\InfiniDB" "InfiniDB_3.6"
IF %ERRORLEVEL% NEQ 0 GOTO ERROR_HANDLER
REN "\InfiniDB_trunk" "InfiniDB"
IF %ERRORLEVEL% NEQ 0 GOTO ERROR_HANDLER
echo Switched to InfiniDB trunk
GOTO COMPLETE

:SWITCH_36
echo switching to 3.6
REN "\InfiniDB" "InfiniDB_trunk"
IF %ERRORLEVEL% NEQ 0 GOTO ERROR_HANDLER
REN "\InfiniDB_3.6" "InfiniDB"
IF %ERRORLEVEL% NEQ 0 GOTO ERROR_HANDLER
echo Switched to InfiniDB 3.6
GOTO COMPLETE

:SWITCH_caldb
echo switching to caldb
REN "\InfiniDB" "InfiniDB_trunk"
IF %ERRORLEVEL% NEQ 0 GOTO ERROR_HANDLER
REN "\InfiniDB_caldb" "InfiniDB"
IF %ERRORLEVEL% NEQ 0 GOTO ERROR_HANDLER
echo Switched to InfiniDB caldb
GOTO COMPLETE

:ERROR_HANDLER
echo failed to switch

:COMPLETE
