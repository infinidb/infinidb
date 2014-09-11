#! /bin/sh
#
# $Id: module_installer.sh 421 2007-04-05 15:46:55Z dhill $
#
# Setup the Custom OS files during a System install on a module
#
#
# append calpont OS files to Linux OS file
#
prefix=/usr/local
#
echo "Check Module File on Module"
if test ! -f $prefix/Calpont/local/module ; then
	echo "FAILED: missing $prefix/Calpont/local/module"
	exit 1
fi

echo "Setup fstab on Module"
DBRootStorageType=`/usr/local/Calpont/bin/getConfig -c /usr/local/Calpont/etc/Calpont.xml Installation DBRootStorageType`
if [ $DBRootStorageType == "storage" ]; then
	sed -i '/^[^#].*\/usr\/local\/Calpont\/data[^ ]/d' /etc/fstab
	cat $prefix/Calpont/local/fstab.calpont >> /etc/fstab
	mount -a > /dev/null 2>&1
fi

echo "Setup rc.local on Module"
if test -f $prefix/Calpont/local/rc.local.calpont ; then
	touch /etc/rc.local
	rm -f /etc/rc.local.calpontSave
	cp /etc/rc.local /etc/rc.local.calpontSave
	cat $prefix/Calpont/local/rc.local.calpont >> /etc/rc.local
fi

echo " "
echo "!!!Module Installation Successfully Completed!!!"

exit 0
