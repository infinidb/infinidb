#!/bin/bash
#
# $Id: install-infinidb.sh 421 2007-04-05 15:46:55Z dhill $
#
# install-infinidb.sh preconfigures a system before the Calpont Database Platform can run

prefix=/usr/local
rpmmode=install

#check 64-bit OS compatiable
arch=`uname -m`
patcnt=`expr "$arch" : 'i.86'`
is64bitos=1
if [ $patcnt -ne 0 ]; then
	is64bitos=0
fi
is64bitpkg=1
file ${prefix}/Calpont/bin/PrimProc | grep '64-bit' >/dev/null 2>&1
if [ $? -ne 0 ]; then
	is64bitpkg=0
fi
if [ $is64bitpkg -eq 1 -a $is64bitos -ne 1 ]; then
	echo "ERROR: Incompatible version, package is intended for a x86_64 architecture"
	echo "exiting...."
	exit 1
fi

#run post-install in case it hadn't been run already
${prefix}/Calpont/bin/post-install --prefix=$prefix --rpmmode=$rpmmode

for arg in "$@"; do
	if [ `expr -- "$arg" : '--prefix='` -eq 9 ]; then
		prefix="`echo $arg | awk -F= '{print $2}'`"
	elif [ `expr -- "$arg" : '--rpmmode='` -eq 10 ]; then
		rpmmode="`echo $arg | awk -F= '{print $2}'`"
	else
		echo "ignoring unknown argument: $arg" 1>&2
	fi
done

netstat -na | egrep -qs ':3306.*LISTEN'
if [ $? -eq 0 ]; then
	egrep -qs '= 3306' ${prefix}/Calpont/mysql/my.cnf
	if [ $? -eq 0 ]; then
		echo " "
		echo " There is already a version of mysqld running."
		echo " If the InfiniDB mysqld is running then stop it and run install-infinidb.sh again."
		echo " If another version of mysqld is running then either change the port number"
		echo " of 3306 in ${prefix}/Calpont/mysql/my.cnf or stop the other mysqld"
		echo " then run install-infinidb.sh again."
		exit 1
	fi
fi

#setup for a single server system
for file in Calpont.xml ProcessConfig.xml snmpd.conf snmptrapd.conf; do
	rm -f ${prefix}/Calpont/etc/${file}.save
	if [ -f ${prefix}/Calpont/etc/${file} ]; then
		mv ${prefix}/Calpont/etc/${file} ${prefix}/Calpont/etc/${file}.save
	fi
	if [ -f ${prefix}/Calpont/etc/${file}.singleserver ]; then
		cp ${prefix}/Calpont/etc/${file}.singleserver ${prefix}/Calpont/etc/${file}
	fi
done

#check 3rd party library check
echo " "
echo "DEPENDENCY LIBRARY CHECK"
echo " "
ldd ${prefix}/Calpont/bin/* | grep 'not found' | sort -u > /tmp/pkgcheck
if [ `cat /tmp/pkgcheck | wc -c` -ne 0 ]; then
		cat /tmp/pkgcheck
        echo " "
        echo "Please install the libraries that are shown to be missing, then run install-infinidb.sh again"
        rm -f /tmp/pkgcheck
        exit 1
fi

rm -f /tmp/pkgcheck
echo "All libraries found"
echo " "

#check disk space
echo "DISK SPACE CHECK"
echo ""
echo "Make sure there is enough local or mounted disk space for the"
echo "InfiniDB System Catalog and the planned test Database."
echo " "
df -h /
df -h ${prefix}/Calpont/data1 2>/dev/null | grep data1
if [ $? -ne 0 ]; then
        echo " "
        echo "No '${prefix}/Calpont/data1' mounted disk found"
        echo "Check the JumpStart Guide for information on how to setup a mounted disk"
        echo "if you require additional disk space for the InfiniDB Database"
fi

echo " "
echo "CONFIGURATION / DATA VALIDATION CHECK"
echo " "
echo "Validates Configuration and Data settings"
echo ""
if [ -f ${prefix}/Calpont/etc/Calpont.xml.rpmsave ]; then
	rm -f ${prefix}/Calpont/etc/Calpont.xml.new
	cp ${prefix}/Calpont/etc/Calpont.xml ${prefix}/Calpont/etc/Calpont.xml.new

	#check FilesPerColumnPartition and ExtentsPerSegmentFile
	oldFilesPer=`${prefix}/Calpont/bin/getConfig -c ${prefix}/Calpont/etc/Calpont.xml.rpmsave ExtentMap FilesPerColumnPartition`
	newFilesPer=`${prefix}/Calpont/bin/getConfig ExtentMap FilesPerColumnPartition`
	
	oldExtentsPer=`${prefix}/Calpont/bin/getConfig -c ${prefix}/Calpont/etc/Calpont.xml.rpmsave ExtentMap ExtentsPerSegmentFile`
	newExtentsPer=`${prefix}/Calpont/bin/getConfig ExtentMap ExtentsPerSegmentFile`
	
	if [ $oldFilesPer -ne $newFilesPer ]; then
			echo ""
			echo "The Configuration Parameter of ExtentMap/FilesPerColumnPartition mis-compares"
			echo "between the Calpont.xml file and the backed up version of Calpont.xml.rpmsave"
			echo "Calpont.xml value = " $oldFilesPer ", Calpont.xml.rpmsave value = " $newFilesPer
			echo ""
			echo "InfiniDB will continue to use the previous value from Calpont.xml.rpmsave"
			echo "Please refer to the JumpStart Guide for additional Information"
			${prefix}/Calpont/bin/setConfig ExtentMap FilesPerColumnPartition $oldFilesPer
	fi
	
	if [ $oldExtentsPer -ne $newExtentsPer ]; then
			echo ""
			echo "The Configuration Parameter of ExtentMap/ExtentsPerSegmentFile mis-compares"
			echo "between the Calpont.xml file and the backed up version of Calpont.xml.rpmsave"
			echo "Calpont.xml value = " $oldExtentsPer ", Calpont.xml.rpmsave value = " $newExtentsPer
			echo ""
			echo "InfiniDB will continue to use the previous value from Calpont.xml.rpmsave"
			echo "Please refer to the JumpStart Guide for additional Information"
			${prefix}/Calpont/bin/setConfig ExtentMap ExtentsPerSegmentFile $oldExtentsPer
	fi

	#check NumBlocksPct and TotalUmMem
	oldNumBlocksPct=`${prefix}/Calpont/bin/getConfig -c ${prefix}/Calpont/etc/Calpont.xml.rpmsave DBBC NumBlocksPct`
	`${prefix}/Calpont/bin/setConfig DBBC NumBlocksPct $oldNumBlocksPct`
	
	oldTotalUmMem=`${prefix}/Calpont/bin/getConfig -c ${prefix}/Calpont/etc/Calpont.xml.rpmsave HashJoin TotalUmMemory`
	`${prefix}/Calpont/bin/setConfig HashJoin TotalUmMemory $oldTotalUmMem`
	
	#check dbrm file location
	dbrmrootPrev=`${prefix}/Calpont/bin/getConfig -c ${prefix}/Calpont/etc/Calpont.xml.rpmsave SystemConfig DBRMRoot`
	dbrmroot=`${prefix}/Calpont/bin/getConfig SystemConfig DBRMRoot`
	
	dbrmrootPrevDir=${dbrmrootPrev//BRM_saves/}
	dbrmrootDir=${dbrmroot//BRM_saves/}
	
	if [ $dbrmrootPrev != $dbrmroot ]; then
		if [ -f $dbrmrootPrev"_current" ]; then
			if [ -f $dbrmroot"_current" ]; then
				echo ""
				echo "DBRM Data File Directory Check"
				echo ""
				echo "**** IMPORTANT PLEASE READ ****************************************************"
				echo ""
				echo "DBRM data files were found in $dbrmrootPrevDir"
				echo "and in new location $dbrmrootDir"
				echo ""
				echo "Make sure that the correct set of files are in the new location."
				echo "Then rename the directory $dbrmrootPrevDir."
				echo "If the files were copied from $dbrmrootPrevDir"
				echo "you will need to edit the file BRM_saves_current to contain the current path of"
				echo $dbrmrootDir
				echo ""
				echo "Please reference the Calpont InfiniDB JumpStart Guide for Upgrade Installs for "
				echo "addition information, if needed."
				echo ""
				echo "*******************************************************************************"
			else
				echo ""
				echo "DBRM Data File Directory Check"
				echo ""
				echo "DBRM data files moved from $dbrmrootPrev"
				echo "to new location $dbrmrootDir."
				echo "Old location $dbrmrootPrevDir renamed to /usr/local/Calpont/dbrm.old ."
				/bin/cp -fp $dbrmrootPrevDir/* $dbrmrootDir/.
				sed 's/dbrm/data1\/systemFiles\/dbrm/' $dbrmrootPrev"_current" > $dbrmroot"_current"
				mv $dbrmrootPrevDir /usr/local/Calpont/dbrm.old
			fi
		fi
	fi
else
	memory=`${prefix}/Calpont/bin/idbmeminfo`
	
	if [ $memory -le 2000 ]; then
			echo "A minumum of 2GB of memory is recommended, which this machine doesn't seem to have."
			echo "We recommend using a machine with more memory."
			echo "If you know you have more memory than shown, see the InfiniDB Tuning Guide for"
			echo "instructions on how to configure InfiniDB's use of system memory."
			value=256M
	elif [ $memory -le 4000 ]; then
			value=512M
	elif [ $memory -le 8000 ]; then
			value=1G
	elif [ $memory -le 16000 ]; then
			value=2G
	elif [ $memory -le 32000 ]; then
			value=4G
	elif [ $memory -le 64000 ]; then
			value=8G
	else
			value=16G
	fi
	
	${prefix}/Calpont/bin/setConfig HashJoin TotalUmMemory $value
fi

NumBlocksPct=`${prefix}/Calpont/bin/getConfig DBBC NumBlocksPct`
TotalUmMemory=`${prefix}/Calpont/bin/getConfig HashJoin TotalUmMemory`

echo "The Memory Configuration Setting are:"
echo " NumBlocksPct = $NumBlocksPct%"
echo " TotalUmMemory = $TotalUmMemory"


echo " "
echo "SETUP INFINIDB MYSQL"
echo " "

${prefix}/Calpont/bin/post-mysqld-install --prefix=$prefix --rpmmode=$rpmmode

#check for password set
#start in the same way that mysqld will be started normally.
/etc/init.d/mysql-Calpont start
sleep 2

df=${prefix}/Calpont/mysql/my.cnf
password=" "
userprompt=" *** Enter MySQL password: "
while true ; do
	$prefix/Calpont/mysql/bin/mysql --defaults-file=$df --force --user=root $pwprompt -e status &>/tmp/idbmysql.log
	
	egrep -qs "ERROR 1045" /tmp/idbmysql.log
	if [ $? -eq 0 ]; then
		echo ""
		stty_orig=`stty -g`
		stty -echo
		echo -n "$userprompt"
		read password
		stty $stty_orig
		echo " "
		pwprompt="--password=$password"
		userprompt=" *** Incorrect password, please re-enter MySQL password: "
	else
		egrep -qs "InfiniDB" /tmp/idbmysql.log
		if [ $? -ne 0 ]; then
			echo ""
			echo "ERROR: MySQL runtime error, exit..."
			cat /tmp/idbmysql.log
			exit 1
		else
			/etc/init.d/mysql-Calpont stop
			rm -f /tmp/idbmysql.log
			break
		fi
	fi
done

${prefix}/Calpont/bin/post-mysql-install --prefix=$prefix --rpmmode=$rpmmode --password=$password

echo " "
echo "SETUP INFINIDB SYSTEM LOGGING"

#setup syslog

${prefix}/Calpont/bin/syslogSetup.sh install

for file in snmpdx.conf; do
	rm -f ${prefix}/Calpont/local/${file}.save
	if [ -f ${prefix}/Calpont/local/${file} ]; then
		mv ${prefix}/Calpont/local/${file} ${prefix}/Calpont/local/${file}.save
	fi
	if [ -f ${prefix}/Calpont/local/${file}.singleserver ]; then
		cp ${prefix}/Calpont/local/${file}.singleserver ${prefix}/Calpont/local/${file}
	fi
done

#create the bulk-load dirs
mkdir -p ${prefix}/Calpont/data/bulk/data/import >/dev/null 2>&1
mkdir -p ${prefix}/Calpont/data/bulk/log >/dev/null 2>&1
mkdir -p ${prefix}/Calpont/data/bulk/job >/dev/null 2>&1
mkdir -p ${prefix}/Calpont/data/bulk/rollback >/dev/null 2>&1

#setup the calpont service script
rm -f /etc/init.d/infinidb
cp ${prefix}/Calpont/bin/infinidb /etc/init.d
if [ -x /sbin/chkconfig ]; then
	/sbin/chkconfig --add infinidb > /dev/null 2>&1
	/sbin/chkconfig infinidb on > /dev/null 2>&1
	echo ""
	echo "InfiniDB is setup for autostart using 'chkconfig'"
elif [ -x /usr/sbin/update-rc.d ]; then
	/usr/sbin/update-rc.d infinidb defaults > /dev/null 2>&1
	echo ""
	echo "InfiniDB is setup for autostart using 'update-rc.d'"
else
	echo ""
	echo "Package 'chkconfig' or 'update-rc.d' not installed, contact your sysadmin if you want to setup to autostart InfiniDB"
fi

#clean up some dirs that post-install created during the rpm install
rm -rf /mnt/parentOAM >/dev/null 2>&1
# if /mnt/tmp is empty, assume that we created it and remove it
cnt=`/bin/ls -1 /mnt/tmp | wc -l`
if [ $cnt -eq 0 ]; then
	rm -rf /mnt/tmp >/dev/null 2>&1
fi

echo " "
echo "InfiniDB Installation Completed"
