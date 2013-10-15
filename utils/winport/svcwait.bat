@echo off

rem wait for InfiniDB service to go to state %1 (RUNNING)
rem dont wait more than about %2 (10) seconds

setlocal
set gotit=0
set count=0
set notinst=0

if x%1 equ x goto :defarg1
set state=%1
goto :gotarg1
:defarg1
set state=RUNNING
:gotarg1

if x%2 equ x goto :defarg2
set max=%2
goto :gotarg2
:defarg2
set max=10
:gotarg2

for /f "usebackq tokens=3" %%a in (`sc.exe query InfiniDB`) do call :checkinst %%a
if %notinst% equ 1 goto :notinst

:mainloop
call :checkstate
if %gotit% equ 1 goto :eof
rem echo waiting...
ping 127.0.0.1 -n 2 -w 1000 > nul
set /a count+=1
if %count% geq %max% goto :timedout
goto :mainloop

:checkstate
for /f "usebackq tokens=1-4" %%a in (`sc.exe query InfiniDB`) do call :cshelper %%a %%d
goto :eof

:cshelper
if %1 neq STATE goto :eof
if %2 neq %state% goto :eof
set gotit=1
goto :eof

:checkinst
if %1 neq FAILED goto :eof
set notinst=1
goto :eof

:timedout
echo Timed-out waiting for InfiniDB to go to state %state%!
exit /b 1
goto :eof

:notinst
exit /b 2
goto :eof

