#!/usr/bin/perl -w
#
# $Id: listRepos.pl 192 2007-05-14 14:42:49Z rdempsey $
#

use File::Basename;

$reporoot = '/Calpont/repos/genii/*';

$opt_a = 0;
if (defined($ENV{'QUERY_STRING'}))
{
	$opt_a = 1 if ($ENV{'QUERY_STRING'} eq "All");
}

sub findRepos
{
	my ($root) = @_;

	my @repolist = ();

	my @repodirs = glob($root);

	my $dir;

	for $dir (@repodirs)
	{
		$dir = basename($dir);

		if ($opt_a == 0)
		{
			next if ($dir eq "net-snmp");
			# deprecated projects
			next if ($dir eq "altera");
			next if ($dir eq "diskmgr");
			next if ($dir eq "emulator");
			next if ($dir eq "message");
			next if ($dir eq "sqlengine");
		}
		next if ($dir eq "demo");
		next if ($dir eq "doc");

		push(@repolist, $dir);
	}

	return @repolist;
}

@repos = findRepos($reporoot);

print 'content-type: text/plain', "\r\n";
print "\r\n";

foreach $repo (@repos)
{
	print $repo, "\r\n";
}

