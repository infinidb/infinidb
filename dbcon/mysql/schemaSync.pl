#!/usr/bin/perl -w
#
# $Id: schemaSync.pl 7431 2011-02-15 14:21:52Z rdempsey $

use DBI;
use DBD::mysql;

$| = 1;

$cfgfile='/usr/local/Calpont/mysql/my.cnf';

$username = 'root';
$auth = '';
%attr = ();

@calpont_objects = ();

sub gather_calpont_objects
{
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
	my $sth = $dbh->prepare($stmt);
	$sth->execute();
	my @row_ary  = $sth->fetchrow_array();
	while ($#row_ary >= 0)
	{
		push(@calpont_objects, [@row_ary]);
		@row_ary  = $sth->fetchrow_array();
	}
}

sub gather_mysql_objects
{
}

sub diff_calpont_mysql
{
}

sub diff_mysql_calpont
{
}

sub conflicts
{
}

$database='calpontsys';
$data_source = 'DBI:mysql:database=' . $database . ':mysql_read_default_file=' . $cfgfile . '';
$dbh = DBI->connect($data_source, $username, $auth, \%attr);
gather_calpont_objects;
$dbh->disconnect;

sub datatype2name
{
	my ($dt) = @_;
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
		$dts = 'clob';
	}
	elsif ($dt == 14)
	{
		$dts = 'blob';
	}
	else
	{
		$dts = "$dt";
	}
	return $dts;
}

%schemas = ();
foreach $co (@calpont_objects)
{
	$schema = "$@$co->[0]";
	$schemas{$schema} = 1;
}

%schematables = ();
foreach $schema (keys %schemas)
{
	foreach $co (@calpont_objects)
	{
		next if ("$@$co->[0]" ne $schema);
		$schematable = $schema . ".$@$co->[1]";
		$schematables{$schematable} = 1;
	}
}

$curdb='';
foreach $schema (keys %schemas)
{
	foreach $schematable (keys %schematables)
	{
		next if (!($schematable =~ /^$schema\./));
		$first = 1;
		foreach $co (@calpont_objects)
		{
			$schtbl = "$@$co->[0].$@$co->[1]";
			next if ($schematable ne $schtbl);
			$ty = "$@$co->[5]";
			$nm = datatype2name($ty);
			$sc = "$@$co->[6]";
			if (($nm eq 'smallint' || $nm eq 'int' || $nm eq 'bigint') && $sc > 0)
			{
				$nm = 'decimal';
			}
			if ($first == 1)
			{
				if ("$@$co->[0]" ne $curdb)
				{
					print "create database if not exists $@$co->[0];\nuse $@$co->[0];\n\n";
					$curdb = "$@$co->[0]";
				}
				print "create table if not exists $@$co->[1] (\n";
				$first = 0;
			}
			else
			{
				print ", ";
			}
			print "$@$co->[2] $nm";
			if ($nm eq 'char' || $nm eq 'varchar')
			{
				print "($@$co->[8])";
			}
			elsif ($nm eq 'decimal')
			{
				print "($@$co->[7],$@$co->[6])";
			}
			print " comment 'compression=$@$co->[10]'\n";
		}
		print ") engine=infinidb comment='schema sync only';\n\n";
	}
}

gather_mysql_objects;
diff_calpont_mysql;
diff_mysql_calpont;
conflicts;

