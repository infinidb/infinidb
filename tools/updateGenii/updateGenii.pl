#!/usr/bin/perl -w
#
# $Id: updateGenii.pl 1265 2010-09-17 12:25:42Z rdempsey $
#

use Getopt::Std;

sub usage
{
	my $msg = <<'EOD'
usage: updateGenii.pl [-h] [-b|-t branch/tag] [-r root] [-a]
   -h        Display this help
   -b branch Update a branch wc
   -t tag    Update a tag wc
   -r root   Use root as the starting point rather than $HOME
   -a        Update wc for all svn projects, even deprecated ones
EOD
;
	print "$msg\n";
}

sub findRepos
{
	my @repolist = ();
	my $all = '';

	$all = '?All' if ($opt_a);

	open(FH, 'curl -s http://srvengcm1.calpont.com/cgi-bin/listRepos.pl' . $all . ' |');
	while (<FH>)
	{
		$_ =~ s/[\r\n]*$//;
		push(@repolist, $_);
	}
	close(FH);

	return @repolist;
}

if (!getopts('hab:r:t:'))
{
	usage();
	exit 1;
}

if (defined($opt_h))
{
	$opt_h = 1;
	usage();
	exit 0;
}

$opt_a = 1 if (defined($opt_a));

$prjroot = 'genii';
$prjroot = $opt_r if (defined($opt_r));
$prjroot = $opt_b if (defined($opt_b));
$prjroot = $opt_t if (defined($opt_t));

if (!defined($opt_b) && !defined($opt_t) && $#ARGV >= 0)
{
	usage();
	exit 1;
}

if (defined($opt_b) || defined($opt_t))
{
	$cmd = 'svn list http://srvengcm1.calpont.com/svn/genii/dbcon/';
	if (defined($opt_b))
	{
		$cmd = $cmd . 'branches';
	}
	else
	{
		$cmd = $cmd . 'tags';
	}
	$listcmd = $cmd;
	$tmp = $prjroot;
	$tmp =~ s/\./\\./g;
	$cmd = $cmd . " 2>/dev/null | egrep '^" . $tmp . "/' | wc -l";
	$brcnt = `$cmd`;
	if ($brcnt != 1)
	{
		print "branch/tag $prjroot not found. Choices are:\n";
		$listcmd = $listcmd . ' | sed s,/\$,,';
		system($listcmd);
		exit 1;
	}
}

chdir $ENV{'HOME'} or die;

$destDir = $prjroot;
if ((defined($opt_b) || defined($opt_t)) && defined($opt_r))
{
	$destDir = $opt_r;
}
mkdir $destDir if (! -d $destDir);

chdir $destDir or die;

@repos = findRepos();
@mergelines = ();
@conflictlines = ();

foreach $repo (@repos)
{
	#print $repo, "\n"; next;
	if (-d $repo . '/.svn')
	{
		print $repo, ": \n";
		chdir $repo or die;
		open FH, 'svn update |';
		@svnlines = <FH>;
		close FH;
		foreach $line (@svnlines)
		{
			print "$line";
			if ($line =~ /^C/)
			{
				push(@conflictlines, $repo . ": " . $line);
			}
			elsif ($line =~ /^G/)
			{
				push(@mergelines, $repo . ": " . $line);
			}
		}

		chdir '..' or die;
	}
	else
	{
		print $repo, ": \n";
		if (defined($opt_b))
		{
			$svncmd = 'svn checkout http://srvengcm1.calpont.com/svn/genii/' . $repo . '/branches/' .
				$prjroot . ' ' . $repo;
		}
		elsif (defined($opt_t))
		{
			$svncmd = 'svn checkout http://srvengcm1.calpont.com/svn/genii/' . $repo . '/tags/' .
				$prjroot . ' ' . $repo;
		}
		else
		{
			$svncmd = 'svn checkout http://srvengcm1.calpont.com/svn/genii/' . $repo . '/trunk/' . ' ' .
				$repo;
		}
		system $svncmd;
	}
}

unlink 'merges.txt';
unlink 'conflicts.txt';

if ($#mergelines >= 0)
{
	print "\n** Warning! merges:\n";
	foreach $merge (@mergelines)
	{
		print "$merge";
	}
	open FH, ">merges.txt";
	print FH @mergelines;
	close FH;
	print "This information has been saved in merges.txt\n";
}

if ($#conflictlines >= 0)
{
	print "\n** Warning! conflicts:\n";
	foreach $conflict (@conflictlines)
	{
		print "$conflict";
	}
	open FH, ">conflicts.txt";
	print FH @conflictlines;
	close FH;
	print "This information has been saved in conflicts.txt\n";
}

if (! -e 'Makefile')
{
	system './build/bootstrap';
}

