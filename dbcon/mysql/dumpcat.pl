#!/usr/bin/perl -w
# $Id: dumpcat.pl 7049 2010-09-14 16:43:13Z rdempsey $
#

use DBI;
use DBD::mysql;

$data_source = 'DBI:mysql:database=calpontsys:mysql_read_default_file=/usr/local/Calpont/mysql/my.cnf';
$username = 'root';
$auth = '';
%attr = ();

$dbh = DBI->connect($data_source, $username, $auth, \%attr);

$stmt = <<EOD
select
        `schema`, tablename, columnname, objectid, dictobjectid, datatype,
        scale, prec, columnlength, columnposition, compressiontype
from
        syscolumn
order by
        `schema`, tablename, columnposition;
EOD
;

print "schema     tablename                 colname           objectid      dict  dt       scale   prec collen  pos  ct\n";
print "------     ---------                 -------           --------  --------  -------- -----   ---- ------  ---  --\n";
format STDOUT =
@<<<<<<<<< @<<<<<<<<<<<<<<<<<<<<<<<< @<<<<<<<<<<<<<<<<< @>>>>>>   @>>>>>>  @<<<<<<< @>>>> @>>>>> @>>>>> @>>> @>>
$schema, $tablename, $colname, $objectid, $dict, $dts, $scale, $prec, $collen, $pos, $ct
.

$sth = $dbh->prepare($stmt);
$sth->execute();
($schema, $tablename, $colname, $objectid, $dict, $dt, $scale, $prec, $collen, $pos, $ct) = $sth->fetchrow_array();
while (defined($schema))
{
	$dict = '' if (!defined($dict));
	if ($dt == 0)
	{
		$dts = 'bit';
	}
	elsif ($dt == 1)
	{
		$dts = 'tinyint';
	}
	elsif ($dt == 2)
	{
		$dts = 'char';
	}
	elsif ($dt == 3)
	{
		$dts = 'smallint';
	}
	elsif ($dt == 4)
	{
		$dts = 'decimal';
	}
	elsif ($dt == 5)
	{
		$dts = 'medint';
	}
	elsif ($dt == 6)
	{
		$dts = 'int';
	}
	elsif ($dt == 7)
	{
		$dts = 'float';
	}
	elsif ($dt == 8)
	{
		$dts = 'date';
	}
	elsif ($dt == 9)
	{
		$dts = 'bigint';
	}
	elsif ($dt == 10)
	{
		$dts = 'double';
	}
	elsif ($dt == 11)
	{
		$dts = 'datetime';
	}
	elsif ($dt == 12)
	{
		$dts = 'varchar';
	}
	elsif ($dt == 13)
	{
		$dts = 'varbinary';
	}
	elsif ($dt == 14)
	{
		$dts = 'clob';
	}
	elsif ($dt == 15)
	{
		$dts = 'blob';
	}
	elsif ($dt == 16)
	{
		$dts = 'utinyint';
	}
	elsif ($dt == 17)
	{
		$dts = 'usmallint';
	}
	elsif ($dt == 18)
	{
		$dts = 'udecimal';
	}
	elsif ($dt == 19)
	{
		$dts = 'umedint';
	}
	elsif ($dt == 20)
	{
		$dts = 'uint';
	}
	elsif ($dt == 21)
	{
		$dts = 'ufloat';
	}
	elsif ($dt == 22)
	{
		$dts = 'ubigint';
	}
	elsif ($dt == 23)
	{
		$dts = 'udouble';
	}
	else
	{
		$dts = "$dt";
	}
	#print "$schema\t$tablename\t$colname\t$objectid\t$dict\t$dt\t$scale\t$prec\t$collen\t$pos\t$ct\n";
	write;
	($schema, $tablename, $colname, $objectid, $dict, $dt, $scale, $prec, $collen, $pos, $ct) = $sth->fetchrow_array();
}

$dbh->disconnect;

