for /f "delims=" %%x in (releasenum.os) do (set "%%x")
set packager="%USERNAME% <%USERNAME%@calpont.com>"
echo %packager%
set builddate="%DATE:~10,4%-%DATE:~4,2%-%DATE:~7,2% %TIME:~0,2%:%TIME:~3,2%:%TIME:~6,2%"
set buildmachine=%computername%
copy /Y .\CalpontVersion.txt.in ..\utils\winport\CalpontVersion.txt
sed -i  -e s/@@PACKAGER@@/%packager%/ ..\utils\winport\CalpontVersion.txt
sed -i  -e s/@@BUILDDATE@@/%builddate%/ ..\utils\winport\CalpontVersion.txt
sed -i  -e s/@@VERSION@@/%version%/ ..\utils\winport\CalpontVersion.txt
sed -i  -e s/@@RELEASE@@/%release%/ ..\utils\winport\CalpontVersion.txt
sed -i  -e s/@@BUILDMACHINE@@/%buildmachine%/ ..\utils\winport\CalpontVersion.txt
:: For some unknown reason, sed removes permissions. Put them back.
icacls ..\utils\winport\CalpontVersion.txt /grant SYSTEM:F administrators:F
cd ..
git status | grep "On branch" >> utils\winport\CalpontVersion.txt
git log -1 | head -3 | egrep "^(commit|Date)" >> utils\winport\CalpontVersion.txt

cd build
