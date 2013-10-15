PATH=C:\PROGRA~2\GNUWIN32\BIN;%PATH%

del ddl-gram-win.h >nul 2>&1
bison -l -v -d -p ddl -o ddl-gram-win.cpp ddl.y
ren ddl-gram-win.hpp ddl-gram-win.h
flex -i -L -Pddl -oddl-scan-win.cpp ddl.l
