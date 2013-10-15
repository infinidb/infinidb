PATH=C:\PROGRA~2\GNUWIN32\BIN;%PATH%

del dml-gram-win.h >nul 2>&1
bison -l -v -d -p dml -o dml-gram-win.cpp dml.y
ren dml-gram-win.hpp dml-gram-win.h
flex -i -L -Pdml -odml-scan-win.cpp dml.l
