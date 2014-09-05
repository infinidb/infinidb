::@echo off

cp getfiles.txt ..\..\mysql-5.1.39\sql
cd ..\..\mysql-5.1.39\sql
for /f %%a in (getfiles.txt) do call :doit %%a

cd ..\include
diff --brief mysql_com.h ..\..\genii\dbcon\mysql\include
if errorlevel 1 call :copyit include\mysql_com.h
diff --brief my_base.h ..\..\genii\dbcon\mysql\include
if errorlevel 1 call :copyit include\my_base.h
diff --brief my_sys.h ..\..\genii\dbcon\mysql\include
if errorlevel 1 call :copyit include\my_sys.h
cd ..\sql
diff --brief errorids.h ..\..\genii\utils\loggingcpp
if errorlevel 1 xcopy ..\..\genii\utils\loggingcpp\errorids.h /y

cd ..\sql
diff --brief errorids.h ..\..\genii\utils\loggingcpp
if errorlevel 1 xcopy ..\..\genii\utils\loggingcpp\errorids.h /y

cd ..\mysys
diff --brief my_handler_errors.h ..\..\genii\dbcon\mysql\mysys
if errorlevel 1 call :copyit mysys\my_handler_errors.h

goto :endit

:doit
diff --brief %1 ..\..\genii\dbcon\mysql
if errorlevel 1 call :copyit %1
goto :eof

:copyit
xcopy ..\..\genii\dbcon\mysql\%1 /y
goto :eof

:endit
cd ..\..\genii\build