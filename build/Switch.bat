::@echo off
IF EXIST C:\InfiniDB_3.6\NUL GOTO SWITCH_36
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

:ERROR_HANDLER
echo failed to switch

:COMPLETE
