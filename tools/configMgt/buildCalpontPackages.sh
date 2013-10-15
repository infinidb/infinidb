#! /bin/sh
#
# $1 - release number or 'Latest' (default 'Latest')
#
if [ "$1" = "" ] ; then
	DIR=Latest
	REL="3.6-0"
elif [ "$1" = "Latest" ] ; then
        DIR=Latest
        REL="3.6-0"
elif [ "$2" = "" ] ; then
	DIR=$1
        REL=$1
else
	DIR=$1
	REL=$2
fi
#
if [[ $REL != *-* ]] ; then
	REL=$REL"-0"
fi
#
{
cd /root/autoOAM/
rpm -e infinidb-libs infinidb-platform infinidb-enterprise --nodeps
rpm -e infinidb-storage-engine infinidb-mysql --nodeps
rpm -e infinidb-mysql --nodeps
rm -rf /usr/local/Calpont
rm -rf calpont*
rm -f *gz
#
smbclient //calweb/shared -Wcalpont -Uoamuser%Calpont1 -c "cd Iterations/$DIR;prompt OFF;mget *.rpm"
rpm -ivh calpont*.x86_64.rpm --nodeps
rpm -iq calpont >> /usr/local/Calpont/releasenum
cd /usr/local/
tar -zcvf calpont-infinidb-ent-$REL.x86_64.bin.tar.gz Calpont
mv calpont-infinidb-ent-$REL.x86_64.bin.tar.gz /root/autoOAM/
cd /root/autoOAM/
alien -ck calpont*.x86_64.rpm
tar -zcvf calpont-infinidb-ent-$REL.x86_64.rpm.tar.gz *$REL*.rpm
tar -zcvf calpont-infinidb-ent-$REL.amd64.deb.tar.gz *$REL*.deb

smbclient //calweb/shared -Wcalpont -Uoamuser%Calpont1 -c "cd Iterations/$DIR;mkdir packages;cd packages;prompt OFF;del calpont-infinidb-ent*gz;mput *gz"
} > /root/autoOAM/buildCalpontPackages-$DIR.log 2>&1
#
echo "Calpont Packages Build Successfully Completed"
exit 0

