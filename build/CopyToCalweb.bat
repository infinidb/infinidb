set dailydirname=genii-%DATE:~10,4%-%DATE:~4,2%-%DATE:~7,2%
xcopy /i /y \InfiniDB\genii\utils\winport\InfiniDB64.exe \\calweb\shared\\Iterations\Latest\packages\
xcopy /i /y \InfiniDB\genii\utils\winport\InfiniDB64.exe \\calweb\shared\\Iterations\nightly\%dailydirname%\packages\
xcopy /i /y \InfiniDB\genii\utils\winport\InfiniDB64-ent.exe \\calweb\shared\\Iterations\Latest\packages\
xcopy /i /y \InfiniDB\genii\utils\winport\InfiniDB64-ent.exe \\calweb\shared\\Iterations\nightly\%dailydirname%\packages\
