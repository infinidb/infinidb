::@echo off

cp getfiles.txt ..\..\mysql-5.1.39\sql
cd ..\..\mysql-5.1.39\sql
for /f %%a in (getfiles.txt) do call :doit %%a

cd ..\include
diff --brief mysql_com.h ..\..\src\dbcon\mysql\include
if errorlevel 1 call :copyit include\mysql_com.h
diff --brief my_base.h ..\..\src\dbcon\mysql\include
if errorlevel 1 call :copyit include\my_base.h
diff --brief my_sys.h ..\..\src\dbcon\mysql\include
if errorlevel 1 call :copyit include\my_sys.h
cd ..\sql
diff --brief errorids.h ..\..\src\utils\loggingcpp
if errorlevel 1 xcopy ..\..\src\utils\loggingcpp\errorids.h /y

cd ..\sql
diff --brief errorids.h ..\..\src\utils\loggingcpp
if errorlevel 1 xcopy ..\..\src\utils\loggingcpp\errorids.h /y

cd ..\mysys
diff --brief my_handler_errors.h ..\..\src\dbcon\mysql\mysys
if errorlevel 1 call :copyit mysys\my_handler_errors.h

goto :endit

:doit
diff --brief %1 ..\..\src\dbcon\mysql
if errorlevel 1 call :copyit %1
goto :eof

:copyit
xcopy ..\..\src\dbcon\mysql\%1 /y
goto :eof

:endit
cd ..\..\src\build