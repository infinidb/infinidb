PATH=C:\PROGRA~2\GNUWIN32\BIN;%PATH%

del bison-win.h >nul 2>&1
bison -d -p qfe -o bison-win.cpp qfeparser.ypp
ren bison-win.hpp bison-win.h
flex -i -Pqfe -olex-win.cpp qfelexer.lpp
