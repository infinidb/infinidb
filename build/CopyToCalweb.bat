echo copying the installer to calweb
set dailydirname=3.6-%DATE:~10,4%-%DATE:~4,2%-%DATE:~7,2%
xcopy /i /y \InfiniDB\genii\utils\winport\InfiniDB64-ent.exe \\calweb\shared\\Iterations\3.6\packages\
xcopy /i /y \InfiniDB\genii\utils\winport\InfiniDB64-ent.exe \\calweb\shared\\Iterations\nightly\%dailydirname%\packages\
