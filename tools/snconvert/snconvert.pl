#!/usr/bin/perl -w -s


###################################
# Control parameters
###################################

# location of the data files to process
$srcDir="";
$srcFileName="";

if ( $#ARGV>-1 )
{
	$srcDir = $ARGV[0];
	$srcFileName= $ARGV[1];
	print "\n\nCLA $srcDir $srcFileName\n\n";
}
else
{
	$srcDir = ".";
	$srcFileName = "FILE000.cdf";
}

# full path of the data file
$srcFile = $srcDir . $srcFileName;

# name of the new file/extent file
$newFileName = $srcFileName . '_';

# get current directory
$currDir = `pwd`;
chomp $currDir;

# number of file system files
$fsFileCnt=8;

# once file data has been reorganized they will be moved
# to these locations.
@pmDest = (
	"root\@srvswdev2\:\/tmp",
	"root\@srvswdev2\:\/tmp",
	"root\@srvswdev2\:\/tmp",
	"root\@srvswdev2\:\/tmp"
);

# split column file into seperate extent files
@sysCmd = ("split", "-d", "-b", "8m", "$srcFile", "$newFileName");
print "@sysCmd\n";
print "Starting split . . . .\n";
system(@sysCmd);
print "Split complete.\n";

# get names of extent files
@files = <$newFileName*>;

$extentCnt = @files;
$i=0;

# list of extent files to place into a file system file
@extList = ("");

#list of file system files
@fsFiles = ("");

#init file lists
while ( $i<$fsFileCnt )
{
	$fsFiles[$i]="fsFile" . $i;
	$extList[$i]="";
	$i++;
}

# organize extents into files per file systems
#in this case there are 8 file systems
$i=0;
$j=0;
while ( $i<$extentCnt )
{
	$j=0;
	while ( $j<$fsFileCnt && $i<$extentCnt )
	{
		$extList[$j] = $extList[$j] . " " . $files[$i];
		$i++;
		$j++;
	}
}

$i=0;
while ( $i < $fsFileCnt )
{
	print "List $i: $extList[$i]\n";
	$Cmd="cat $extList[$i] >> $fsFiles[$i]";
	print "concat $i: $Cmd\n";
	`$Cmd`;
	$i++;
}

# copy the file system files to the appropriate PM
# in this case there are 4 PMs
$pmCnt=4;
$i=0;
$j=0;
@pmFiles =("");

while ( $i<$fsFileCnt )
{
	$j=0;
	while ( $j < $pmCnt  && $i < $fsFileCnt)
	{
#		$pmFiles[$j] = $pmFiles[$j] . " " . $fsFiles[$i++];
		$Cmd = "./remote_scp_put.sh $fsFiles[$i++] $pmDest[$j++] Calpont1";
		print "scp command: $Cmd\n";
		`$Cmd`;
	}
}

#$j=0;
#while ( $j < $pmCnt )
#{
#	print "pmFileList $j : $pmFiles[$j]\n";
#	$j++;
#}
#
#$j=0;
#while ( $j < $pmCnt )
#{
#	$Cmd = "scp $pmFiles[$j] $pmDest[$j]";
#	print "scp command: $Cmd\n";
#	system($Cmd);
#	$j++;
#}

