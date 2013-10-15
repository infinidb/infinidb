#! /bin/bash
# updatePassword.sh
# 

#check command
if [ "$1" = "" ]; then
	echo "Enter New Password"
	exit 1
fi

#get password hash without '/' in it
while true ; do
	pwHash=`openssl passwd -1 ${1}`
	if [[ "$pwHash" =~ / ]]; then
		i=1
	else
		break
	fi
done

cp /etc/shadow /etc/shadow.orig 2> /dev/null
/bin/cp -f /etc/shadow /etc/shadow.save 2> /dev/null

sed -e '/root:/s/:[^:]*:/:'"$pwHash"':/' /etc/shadow.save > /etc/shadow
if [ $? -ne 0 ]; then
	exit 1
fi

exit 0
