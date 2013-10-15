#! /usr/bin/perl -w
#
# $Id: nulltests.pl 5967 2009-10-21 14:56:59Z rdempsey $

use DBI;
use DBD::mysql;

$| = 1;

$database='rjd';
$cfgfile='/home/rdempsey/mysql/etc/my.cnf';

$data_source = 'DBI:mysql:database=' . $database . ':mysql_read_default_file=' . $cfgfile . '';
$username = 'root';
$auth = '';
%attr = ();

$dbh = DBI->connect($data_source, $username, $auth, \%attr);

sub create_table
{
	my ($dbh, $tn, $colttype) = @_;
	my $stmt = "create table " . $tn . " (col1 " . $colttype . ") engine=infinidb;";
	$dbh->do($stmt);
}

sub drop_table
{
	my ($dbh, $tn) = @_;
	my $stmt = "drop table " . $tn . ";";
	$dbh->do($stmt);
}

sub insert_int_rows
{
	my ($dbh, $tn) = @_;
	my $stmt = "insert into " . $tn . " values (null);";
	$dbh->do($stmt);
	$stmt = "insert into " . $tn . " values (1);";
	$dbh->do($stmt);
}

sub insert_char_rows
{
	my ($dbh, $tn) = @_;
	my $stmt = "insert into " . $tn . " values (null);";
	$dbh->do($stmt);
	$stmt = "insert into " . $tn . " values ('');";
	$dbh->do($stmt);
	$stmt = "insert into " . $tn . " values ('A');";
	$dbh->do($stmt);
}

sub insert_date_rows
{
	my ($dbh, $tn) = @_;
	my $stmt = "insert into " . $tn . " values (null);";
	$dbh->do($stmt);
	$stmt = "insert into " . $tn . " values ('2008-05-14');";
	$dbh->do($stmt);
}

sub check_int_counts
{
	my ($dbh, $tn) = @_;

	my $stmt = "select count(*) from " . $tn . ";";
	my @row_ary  = $dbh->selectrow_array($stmt);
	die if ($#row_ary != 0);
	my $cnt = $row_ary[0];
	die if ($cnt != 2);

	$stmt = "select count(*) from " . $tn . " where col1 is null;";
	@row_ary  = $dbh->selectrow_array($stmt);
	die if ($#row_ary != 0);
	$cnt = $row_ary[0];
	die if ($cnt != 1);

	$stmt = "select count(*) from " . $tn . " where col1 is not null;";
	@row_ary  = $dbh->selectrow_array($stmt);
	die if ($#row_ary != 0);
	$cnt = $row_ary[0];
	die if ($cnt != 1);

	$stmt = "select count(*) from " . $tn . " where col1 = 1;";
	@row_ary  = $dbh->selectrow_array($stmt);
	die if ($#row_ary != 0);
	$cnt = $row_ary[0];
	die if ($cnt != 1);

	$stmt = "select count(*) from " . $tn . " where col1 <> 1;";
	@row_ary  = $dbh->selectrow_array($stmt);
	die if ($#row_ary != 0);
	$cnt = $row_ary[0];
	die if ($cnt != 0);

	$stmt = "select count(*) from " . $tn . " where col1 < 127;";
	@row_ary  = $dbh->selectrow_array($stmt);
	die if ($#row_ary != 0);
	$cnt = $row_ary[0];
	die if ($cnt != 1);

	$stmt = "select count(*) from " . $tn . " where col1 > -126;";
	@row_ary  = $dbh->selectrow_array($stmt);
	die if ($#row_ary != 0);
	$cnt = $row_ary[0];
	die if ($cnt != 1);

}

sub check_char_counts
{
	my ($dbh, $tn) = @_;

	my $stmt = "select count(*) from " . $tn . ";";
	my @row_ary  = $dbh->selectrow_array($stmt);
	die if ($#row_ary != 0);
	my $cnt = $row_ary[0];
	die if ($cnt != 3);

	$stmt = "select count(*) from " . $tn . " where col1 is null;";
	@row_ary  = $dbh->selectrow_array($stmt);
	die if ($#row_ary != 0);
	$cnt = $row_ary[0];
	die if ($cnt != 2);

	$stmt = "select count(*) from " . $tn . " where col1 is not null;";
	@row_ary  = $dbh->selectrow_array($stmt);
	die if ($#row_ary != 0);
	$cnt = $row_ary[0];
	die if ($cnt != 1);

	$stmt = "select count(*) from " . $tn . " where col1 = 'A';";
	@row_ary  = $dbh->selectrow_array($stmt);
	die if ($#row_ary != 0);
	$cnt = $row_ary[0];
	die if ($cnt != 1);

	$stmt = "select count(*) from " . $tn . " where col1 <> 'A';";
	@row_ary  = $dbh->selectrow_array($stmt);
	die if ($#row_ary != 0);
	$cnt = $row_ary[0];
	die if ($cnt != 0);

	$stmt = "select count(*) from " . $tn . " where col1 < 'Z';";
	@row_ary  = $dbh->selectrow_array($stmt);
	die if ($#row_ary != 0);
	$cnt = $row_ary[0];
	die if ($cnt != 1);

	$stmt = "select count(*) from " . $tn . " where col1 > '!';";
	@row_ary  = $dbh->selectrow_array($stmt);
	die if ($#row_ary != 0);
	$cnt = $row_ary[0];
	die if ($cnt != 1);

}

sub check_date_counts
{
	my ($dbh, $tn) = @_;

	my $stmt = "select count(*) from " . $tn . ";";
	my @row_ary  = $dbh->selectrow_array($stmt);
	die if ($#row_ary != 0);
	my $cnt = $row_ary[0];
	die if ($cnt != 2);

	$stmt = "select count(*) from " . $tn . " where col1 is null;";
	@row_ary  = $dbh->selectrow_array($stmt);
	die if ($#row_ary != 0);
	$cnt = $row_ary[0];
	die if ($cnt != 1);

	$stmt = "select count(*) from " . $tn . " where col1 is not null;";
	@row_ary  = $dbh->selectrow_array($stmt);
	die if ($#row_ary != 0);
	$cnt = $row_ary[0];
	die if ($cnt != 1);

	$stmt = "select count(*) from " . $tn . " where col1 = '2008-05-14';";
	@row_ary  = $dbh->selectrow_array($stmt);
	die if ($#row_ary != 0);
	$cnt = $row_ary[0];
	die if ($cnt != 1);

	$stmt = "select count(*) from " . $tn . " where col1 <> '2008-05-14';";
	@row_ary  = $dbh->selectrow_array($stmt);
	die if ($#row_ary != 0);
	$cnt = $row_ary[0];
	die if ($cnt != 0);

	$stmt = "select count(*) from " . $tn . " where col1 < '2018-05-14';";
	@row_ary  = $dbh->selectrow_array($stmt);
	die if ($#row_ary != 0);
	$cnt = $row_ary[0];
	die if ($cnt != 1);

	$stmt = "select count(*) from " . $tn . " where col1 > '1998-05-14';";
	@row_ary  = $dbh->selectrow_array($stmt);
	die if ($#row_ary != 0);
	$cnt = $row_ary[0];
	die if ($cnt != 1);

}

sub dotinyinttest
{
	my ($dbh) = @_;
	my $tn = 'tinyinttest';
	print "running tinyint tests...";
	create_table($dbh, $tn, 'tinyint');
	insert_int_rows($dbh, $tn);
	check_int_counts($dbh, $tn);
	drop_table($dbh, $tn);
	print "done\n";
}

sub dosmallinttest
{
	my ($dbh) = @_;
	my $tn = 'smallinttest';
	print "running smallint tests...";
	create_table($dbh, $tn, 'smallint');
	insert_int_rows($dbh, $tn);
	check_int_counts($dbh, $tn);
	drop_table($dbh, $tn);
	print "done\n";
}

sub dointtest
{
	my ($dbh) = @_;
	my $tn = 'inttest';
	print "running int tests...";
	create_table($dbh, $tn, 'int');
	insert_int_rows($dbh, $tn);
	check_int_counts($dbh, $tn);
	drop_table($dbh, $tn);
	print "done\n";
}

sub dobiginttest
{
	my ($dbh) = @_;
	my $tn = 'biginttest';
	print "running bigint tests...";
	create_table($dbh, $tn, 'bigint');
	insert_int_rows($dbh, $tn);
	check_int_counts($dbh, $tn);
	drop_table($dbh, $tn);
	print "done\n";
}

sub dochar1test
{
	my ($dbh) = @_;
	my $tn = 'char1test';
	print "running char1 tests...";
	create_table($dbh, $tn, 'char(1)');
	insert_char_rows($dbh, $tn);
	check_char_counts($dbh, $tn);
	drop_table($dbh, $tn);
	print "done\n";
}

sub dochar2test
{
	my ($dbh) = @_;
	my $tn = 'char2test';
	print "running char2 tests...";
	create_table($dbh, $tn, 'char(2)');
	insert_char_rows($dbh, $tn);
	check_char_counts($dbh, $tn);
	drop_table($dbh, $tn);
	print "done\n";
}

sub dochar4test
{
	my ($dbh) = @_;
	my $tn = 'char4test';
	print "running char4 tests...";
	create_table($dbh, $tn, 'char(4)');
	insert_char_rows($dbh, $tn);
	check_char_counts($dbh, $tn);
	drop_table($dbh, $tn);
	print "done\n";
}

sub dochar8test
{
	my ($dbh) = @_;
	my $tn = 'char8test';
	print "running char8 tests...";
	create_table($dbh, $tn, 'char(8)');
	insert_char_rows($dbh, $tn);
	check_char_counts($dbh, $tn);
	drop_table($dbh, $tn);
	print "done\n";
}

sub dovarchar1test
{
	my ($dbh) = @_;
	my $tn = 'varchar1test';
	print "running varchar1 tests...";
	create_table($dbh, $tn, 'varchar(1)');
	insert_char_rows($dbh, $tn);
	check_char_counts($dbh, $tn);
	drop_table($dbh, $tn);
	print "done\n";
}

sub dovarchar3test
{
	my ($dbh) = @_;
	my $tn = 'varchar3test';
	print "running varchar3 tests...";
	create_table($dbh, $tn, 'varchar(3)');
	insert_char_rows($dbh, $tn);
	check_char_counts($dbh, $tn);
	drop_table($dbh, $tn);
	print "done\n";
}

sub dovarchar7test
{
	my ($dbh) = @_;
	my $tn = 'varchar7test';
	print "running varchar7 tests...";
	create_table($dbh, $tn, 'varchar(7)');
	insert_char_rows($dbh, $tn);
	check_char_counts($dbh, $tn);
	drop_table($dbh, $tn);
	print "done\n";
}

sub dochar40test
{
	my ($dbh) = @_;
	my $tn = 'char40test';
	print "running char40 tests...";
	create_table($dbh, $tn, 'char(40)');
	insert_char_rows($dbh, $tn);
	check_char_counts($dbh, $tn);
	drop_table($dbh, $tn);
	print "done\n";
}

sub dovarchar40test
{
	my ($dbh) = @_;
	my $tn = 'varchar40test';
	print "running varchar40 tests...";
	create_table($dbh, $tn, 'varchar(40)');
	insert_char_rows($dbh, $tn);
	check_char_counts($dbh, $tn);
	drop_table($dbh, $tn);
	print "done\n";
}

sub dodec4test
{
	my ($dbh) = @_;
	my $tn = 'dec4test';
	print "running decimal4 tests...";
	create_table($dbh, $tn, 'decimal(4,2)');
	insert_int_rows($dbh, $tn);
	check_int_counts($dbh, $tn);
	drop_table($dbh, $tn);
	print "done\n";
}

sub dodec9test
{
	my ($dbh) = @_;
	my $tn = 'dec9test';
	print "running decimal9 tests...";
	create_table($dbh, $tn, 'decimal(9,2)');
	insert_int_rows($dbh, $tn);
	check_int_counts($dbh, $tn);
	drop_table($dbh, $tn);
	print "done\n";
}

sub dodec18test
{
	my ($dbh) = @_;
	my $tn = 'dec18test';
	print "running decimal18 tests...";
	create_table($dbh, $tn, 'decimal(18,2)');
	insert_int_rows($dbh, $tn);
	check_int_counts($dbh, $tn);
	drop_table($dbh, $tn);
	print "done\n";
}

sub dodatetest
{
	my ($dbh) = @_;
	my $tn = 'datetest';
	print "running date tests...";
	create_table($dbh, $tn, 'date');
	insert_date_rows($dbh, $tn);
	check_date_counts($dbh, $tn);
	drop_table($dbh, $tn);
	print "done\n";
}

sub dodatetimetest
{
	my ($dbh) = @_;
	my $tn = 'datetimetest';
	print "running datetime tests...";
	create_table($dbh, $tn, 'datetime');
	insert_date_rows($dbh, $tn);
	check_date_counts($dbh, $tn);
	drop_table($dbh, $tn);
	print "done\n";
}

sub dofloattest
{
	my ($dbh) = @_;
	my $tn = 'floattest';
	print "running float tests...";
	create_table($dbh, $tn, 'float');
	insert_int_rows($dbh, $tn);
	check_int_counts($dbh, $tn);
	drop_table($dbh, $tn);
	print "done\n";
}

sub dodoubletest
{
	my ($dbh) = @_;
	my $tn = 'doubletest';
	print "running double tests...";
	create_table($dbh, $tn, 'double');
	insert_int_rows($dbh, $tn);
	check_int_counts($dbh, $tn);
	drop_table($dbh, $tn);
	print "done\n";
}

dotinyinttest($dbh);
dosmallinttest($dbh);
dointtest($dbh);
dobiginttest($dbh);

dochar1test($dbh);
dochar2test($dbh);
dochar4test($dbh);
dochar8test($dbh);

dovarchar1test($dbh);
dovarchar3test($dbh);
dovarchar7test($dbh);

dochar40test($dbh);
dovarchar40test($dbh);

dodec4test($dbh);
dodec9test($dbh);
dodec18test($dbh);
#dodec30test($dbh);

dodatetest($dbh);
dodatetimetest($dbh);

dofloattest($dbh);
dodoubletest($dbh);

$dbh->disconnect;

