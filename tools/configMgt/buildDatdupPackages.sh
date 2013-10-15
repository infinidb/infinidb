#! /bin/sh
#
# $1 - release number or 'Latest' (default 'Latest')
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
rpm -e infinidb-libs infinidb-platform infinidb-enterprise --nodeps --allmatches
rpm -e infinidb-storage-engine infinidb-mysql --nodeps --allmatches
rpm -e infinidb-mysql --nodeps --allmatches
rpm -e infinidb-datdup --nodeps --allmatches
rm -rf /usr/local/Calpont
rm -rf infinidb*
rm -f *gz
#
smbclient //calweb/shared -Wcalpont -Uoamuser%Calpont1 -c "cd Iterations/$DIR/packages;prompt OFF;mget infinidb-datdup*.rpm"
#
test -f infinidb-datdup*.rpm || echo "infinidb-datdup rpm"
test -f infinidb-datdup*.rpm || exit -1
#
rpm -ivh infinidb-datdup*.x86_64.rpm --nodeps
cd /usr/local/
tar -zcvf infinidb-datdup-$REL.x86_64.bin.tar.gz Calpont
mv infinidb-datdup-$REL.x86_64.bin.tar.gz /root/autoOAM/
cd /root/autoOAM/
smbclient //calweb/shared -Wcalpont -Uoamuser%Calpont1 -c "cd Iterations/$DIR/packages;prompt OFF;del infinidb-datdup*gz;mput *gz"
} > /root/autoOAM/buildDatdupPackages-$DIR.log 2>&1
#
echo "DatDup Package Build Successfully Completed"
exit 0

