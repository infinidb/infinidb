#!/usr/bin/perl -w
# $Id: fdtests.pl 5967 2009-10-21 14:56:59Z rdempsey $
#

use DBI;
use DBD::mysql;

$databasename='calpont';
$cnffile='/usr/local/Calpont/mysql/my.cnf';

$data_source = 'DBI:mysql:database=' . $databasename . ':mysql_read_default_file=' . $cnffile . '';
$username = 'root';
$auth = '';
%attr = ();

$dbh = DBI->connect($data_source, $username, $auth, \%attr);

sub create_table
{
	my ($dbh, $tn) = @_;
	my $stmt = "create table " . $tn . " (col1 float, col2 double) engine=infinidb;";
	$dbh->do($stmt);
}

sub drop_table
{
	my ($dbh, $tn) = @_;
	my $stmt = "drop table " . $tn . ";";
	$dbh->do($stmt);
}

sub insert_rows
{
	my ($dbh, $tn) = @_;
	my $stmt;
	$dbh->do("set autocommit=off;");

	$stmt = "insert into " . $tn . " values (4.04e20, 4.04e250);"; $dbh->do($stmt);
	$stmt = "insert into " . $tn . " values (3.03, 3.03);"; $dbh->do($stmt);
	$stmt = "insert into " . $tn . " values (2.02, 2.02);"; $dbh->do($stmt);
	$stmt = "insert into " . $tn . " values (1.01, 1.01);"; $dbh->do($stmt);
	$stmt = "insert into " . $tn . " values (0, 0);"; $dbh->do($stmt);
	$stmt = "insert into " . $tn . " values (-1.01, -1.01);"; $dbh->do($stmt);
	$stmt = "insert into " . $tn . " values (-2.02, -2.02);"; $dbh->do($stmt);
	$stmt = "insert into " . $tn . " values (-3.03, -3.03);"; $dbh->do($stmt);
	$stmt = "insert into " . $tn . " values (-4.04e20, -4.04e250);"; $dbh->do($stmt);
	$stmt = "insert into " . $tn . " values (null, null);"; $dbh->do($stmt);
	$stmt = "insert into " . $tn . " values (3.14159265358979323846, 3.14159265358979323846);"; $dbh->do($stmt);

	$dbh->do("commit;");
	$dbh->do("set autocommit=on;");
}

sub run_tests
{
	my ($dbh, $tn) = @_;
	my $stmt;
	my @row_ary;
	my $cnt;

	$stmt = "select count(*) from " . $tn . ";";
	@row_ary = $dbh->selectrow_array($stmt);
	die if ($#row_ary != 0);
	$cnt = $row_ary[0];
	die if ($cnt != 11);

	$stmt = "select count(*) from " . $tn . " where col1 < 4.05e20;";
	@row_ary = $dbh->selectrow_array($stmt);
	die if ($#row_ary != 0);
	$cnt = $row_ary[0];
	die if ($cnt != 10);

	$stmt = "select count(*) from " . $tn . " where col1 < 0.99;";
	@row_ary = $dbh->selectrow_array($stmt);
	die if ($#row_ary != 0);
	$cnt = $row_ary[0];
	die if ($cnt != 5);

	$stmt = "select count(*) from " . $tn . " where col1 < -2.03;";
	@row_ary = $dbh->selectrow_array($stmt);
	die if ($#row_ary != 0);
	$cnt = $row_ary[0];
	die if ($cnt != 2);

	$stmt = "select count(*) from " . $tn . " where col1 < -4.05e20;";
	@row_ary = $dbh->selectrow_array($stmt);
	die if ($#row_ary != 0);
	$cnt = $row_ary[0];
	die if ($cnt != 0);

	$stmt = "select count(*) from " . $tn . " where col1 between 0 - 0.0005 and 0 + 0.0005;";
	@row_ary = $dbh->selectrow_array($stmt);
	die if ($#row_ary != 0);
	$cnt = $row_ary[0];
	die if ($cnt != 1);

	$stmt = "select count(*) from " . $tn . " where col2 < 4.05e250;";
	@row_ary = $dbh->selectrow_array($stmt);
	die if ($#row_ary != 0);
	$cnt = $row_ary[0];
	die if ($cnt != 10);

	$stmt = "select count(*) from " . $tn . " where col2 < 0.99;";
	@row_ary = $dbh->selectrow_array($stmt);
	die if ($#row_ary != 0);
	$cnt = $row_ary[0];
	die if ($cnt != 5);

	$stmt = "select count(*) from " . $tn . " where col2 < -2.03;";
	@row_ary = $dbh->selectrow_array($stmt);
	die if ($#row_ary != 0);
	$cnt = $row_ary[0];
	die if ($cnt != 2);

	$stmt = "select count(*) from " . $tn . " where col2 < -4.05e250;";
	@row_ary = $dbh->selectrow_array($stmt);
	die if ($#row_ary != 0);
	$cnt = $row_ary[0];
	die if ($cnt != 0);

	$stmt = "select count(*) from " . $tn . " where col2 between 0 - 0.0005 and 0 + 0.0005;";
	@row_ary = $dbh->selectrow_array($stmt);
	die if ($#row_ary != 0);
	$cnt = $row_ary[0];
	die if ($cnt != 1);

	$stmt = "select count(*) from " . $tn . " where col1 between 3.14159 - 0.00001 and 3.14159 + 0.00001 ;";
	@row_ary = $dbh->selectrow_array($stmt);
	die if ($#row_ary != 0);
	$cnt = $row_ary[0];
	die if ($cnt != 1);

	$stmt = "select count(*) from " . $tn .  " where col2 between " .
		"3.14159265358979 - 0.00000000000001 and 3.14159265358979 + 0.00000000000001;";
	@row_ary = $dbh->selectrow_array($stmt);
	die if ($#row_ary != 0);
	$cnt = $row_ary[0];
	die if ($cnt != 1);

	$stmt = "select sum(col1) from " . $tn . ";";
	@row_ary = $dbh->selectrow_array($stmt);
	die if ($#row_ary != 0);
	$cnt = $row_ary[0];
	die if ($cnt < 3.14159 - 0.00001 || $cnt > 3.14159 + 0.00001);

	$stmt = "select min(col1) from " . $tn . ";";
	@row_ary = $dbh->selectrow_array($stmt);
	die if ($#row_ary != 0);
	$cnt = $row_ary[0];
	die if ($cnt < -4.04e20 - 0.01e20 || $cnt > -4.04e20 + 0.01e20);

	$stmt = "select max(col1) from " . $tn . ";";
	@row_ary = $dbh->selectrow_array($stmt);
	die if ($#row_ary != 0);
	$cnt = $row_ary[0];
	die if ($cnt < 4.04e20 - 0.01e20 || $cnt > 4.04e20 + 0.01e20);

	$stmt = "select avg(col1) from " . $tn . ";";
	@row_ary = $dbh->selectrow_array($stmt);
	die if ($#row_ary != 0);
	$cnt = $row_ary[0];
	die if ($cnt < 0.314159 - 0.000001 || $cnt > 0.314159 + 0.000001);

	$stmt = "select sum(col2) from " . $tn . ";";
	@row_ary = $dbh->selectrow_array($stmt);
	die if ($#row_ary != 0);
	$cnt = $row_ary[0];
	die if ($cnt < 3.14159 - 0.00001 || $cnt > 3.14159 + 0.00001);

	$stmt = "select min(col2) from " . $tn . ";";
	@row_ary = $dbh->selectrow_array($stmt);
	die if ($#row_ary != 0);
	$cnt = $row_ary[0];
	die if ($cnt < -4.04e250 - 0.01e250 || $cnt > -4.04e250 + 0.01e250);

	$stmt = "select max(col2) from " . $tn . ";";
	@row_ary = $dbh->selectrow_array($stmt);
	die if ($#row_ary != 0);
	$cnt = $row_ary[0];
	die if ($cnt < 4.04e250 - 0.01e250 || $cnt > 4.04e250 + 0.01e250);

	$stmt = "select avg(col2) from " . $tn . ";";
	@row_ary = $dbh->selectrow_array($stmt);
	die if ($#row_ary != 0);
	$cnt = $row_ary[0];
	die if ($cnt < 0.314159 - 0.000001 || $cnt > 0.314159 + 0.000001);

}

create_table($dbh, 'fdtest');

insert_rows($dbh, 'fdtest');

run_tests($dbh, 'fdtest');

drop_table($dbh, 'fdtest');

$dbh->disconnect;

