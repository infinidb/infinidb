::@echo off
IF NOT EXIST C:\InfiniDB_3.6\NUL GOTO SWITCH_to_trunk
IF NOT EXIST C:\InfiniDB_4.0\NUL GOTO SWITCH_to_3.6
IF NOT EXIST C:\InfiniDB_trunk\NUL GOTO SWITCH_to_4.0

:SWITCH_to_trunk
echo switching to trunk
REN "\InfiniDB" "InfiniDB_3.6"
IF %ERRORLEVEL% NEQ 0 GOTO ERROR_HANDLER
REN "\InfiniDB_trunk" "InfiniDB"
IF %ERRORLEVEL% NEQ 0 GOTO ERROR_HANDLER
echo Switched to InfiniDB trunk
GOTO COMPLETE

:SWITCH_to_3.6
echo switching to 3.6
REN "\InfiniDB" "InfiniDB_4.0"
IF %ERRORLEVEL% NEQ 0 GOTO ERROR_HANDLER
REN "\InfiniDB_3.6" "InfiniDB"
IF %ERRORLEVEL% NEQ 0 GOTO ERROR_HANDLER
echo Switched to InfiniDB 3.6
GOTO COMPLETE

:SWITCH_to_4.0
echo switching to 4.0
REN "\InfiniDB" "InfiniDB_trunk"
IF %ERRORLEVEL% NEQ 0 GOTO ERROR_HANDLER
REN "\InfiniDB_4.0" "InfiniDB"
IF %ERRORLEVEL% NEQ 0 GOTO ERROR_HANDLER
echo Switched to InfiniDB 4.0
GOTO COMPLETE

:ERROR_HANDLER
echo failed to switch

:COMPLETE
