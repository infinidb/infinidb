#!/usr/bin/env perl
#
#-------------------------------------------------------------------------------
#
# $Id$
#
# Copyright (C) 2009-2012 Calpont Corporation
# All rights reserved
#
#-------------------------------------------------------------------------------
#
# Purpose:
#   This script takes a syslog file as input (from STDIN), and extracts
#   and formats the information from message types 26-32 into a report
#   (written to STDOUT).  These message types contain the information
#   necessary to report the elapsed time and block I/O counts for the
#   job steps associated with each SQL statement found in the syslog.
#   The resulting report thus provides profile information regarding
#   the performance of the calpont database.
#
#   The script parses these log messages, and collects the data into a set
#   of hash tables.  When all the job steps for a statement are completed,
#   the script will calculate and report elapsed times, and I/O block counts
#   (per object), for each of these steps.
#
#   Usage:
#     dbprof [-h] [-s sessionId] [-d] logfile
#       -h       displays this help message
#       -s       id of session to be extracted from syslog file
#       -d       enables debug mode
#       logfile  name of log file to be read
#
#   Supported Message Types:
#
#     Message  Message Description and Format
#     Type     (as defined in MessageFile.txt
#     -------  ------------------------------
#     26       Start Transaction
#     27       End Transaction: xxx
#     28       Start Statement: Statement-999 Ver-999 SQL-yyy
#     29       End Statement: Statement-999
#     30       Start Step: Statement-999 StepId-999 StepName-zzz
#     31       End Step: Statement-999 StepId-999
#                   BlockedFifoIn-999 BlockedFifoOut-999
#                   MsgBytesIn-999 MsgBytesOut-999
#     32       I/O: Statement-999 StepId-999 ObjectId-999
#                   PhyRead-999 CacheRead-999 CPBlks-999
#     46       ProcessingTimesi: Statement-999 StepId-999
#                   FirstRead-hh:mm:ss.xxxxxx LastRead-hh:mm:ss.xxxxxx
#                   FirstWrite-hh:mm:ss.xxxxxx LastWrite-hh:mm:ss.xxxxxx
#     47       Summary: Statement-999 MaxMemPct-99 TempFileCnt-999
#                   TempFileSpace-999 PhysRead-999 CacheRead-999
#                   MsgsRcvd-999 MsgBytesIn-999 MsgBytesOut-999
#                   CPBlks-999
#
#     Where:
#           999 represents numeric field values.
#           xxx in message 27 is "COMMIT" or "ROLLBACK".
#           xxx in message 28 is the actual SQL statement.
#           zzz in message 30 is a short descriptive title for the step.
#
# WARNING:
#   This script is very dependent on the format and content of the syslog
#   file, and more specifically, the messages being parsed.  If any changes
#   should be made to the format of the message body for the pertinent
#   messages, or to some other part of the log entries (such as to the date
#   format), then this script may be affected.
#
# TBD:
#   As time allows, it would be helpful to add a utility that could be
#   called, to convert the object id (from the syslog) to an object
#   name.  We could then display the object name in the report in addition
#   to, or instead of, the object id.
#
#-------------------------------------------------------------------------------
use warnings;
use strict;

use POSIX;
use Text::Wrap;
use Time::Local;

#-------------------------------------------------------------------------------
# Global constants
#-------------------------------------------------------------------------------
my $LOG_MSG_NUM_START_TRANS     = 26;  # Start Transaction message type
my $LOG_MSG_NUM_END_TRANS       = 27;  # End   Transaction message type
my $LOG_MSG_NUM_START_STATEMENT = 28;  # Start Statement   message type
my $LOG_MSG_NUM_END_STATEMENT   = 29;  # End   Statement   message type
my $LOG_MSG_NUM_START_STEP      = 30;  # Start Step        message type
my $LOG_MSG_NUM_STOP_STEP       = 31;  # End   Step        message type
my $LOG_MSG_NUM_IO_COUNT        = 32;  # Object Read I/O   message type
my $LOG_MSG_NUM_PROC_TIMES      = 46;  # Step Processing Times msg type
my $LOG_MSG_NUM_STATEMENT_SUM   = 47;  # Statement Summary message type
my $KEY_CONCATENATE_CHAR        = "_"; # character used in concatenating
                                       # host/session hash key string
my $SQL_LINE_LENGTH             =120;  # maximum SQL wrap line length
my $NUM_OBJECTS_TO_REPORT       =  2;  # number of objects to report per step
my $NUM_OBJECTS_TO_RPT_PER_LINE =  1;  # number of objects to report per line
my $SESSION_ID_WILDCARD         = -1;  # indicates to include all session ids
my $EMPTY_STRING                = "-"; # used to denote empty string

my %MON_ALPHA_TO_NUM = (Jan => 0,
                        Feb => 1,
                        Mar => 2,
                        Apr => 3,
                        May => 4,
                        Jun => 5,
                        Jul => 6,
                        Aug => 7,
                        Sep => 8,
                        Oct => 9,
                        Nov =>10,
                        Dec =>11);

#-------------------------------------------------------------------------------
# Global variable declarations
#-------------------------------------------------------------------------------
my $debugFlag = 0;                     # debug flag taken from command line
my $sessionToExtract = $SESSION_ID_WILDCARD; # id of session to be extracted
my $timeStampYear;                     # year used in time stamps, see
                                       # getCurrentYear()
my $logFileName = "";                  # name of log file to be read

# Global variables used to store contents parsed from the latest syslog
# record.  Went ahead and made these global to avoid having to pass around
# among several subroutines. 
my $current_logRecord;                 # current log record read from <STDIN>
my $current_monAlpha;                  # current log month (3 char abbreviation)
my $current_mday;                      # current log month (from 0 to 11)
my $current_hh;                        # current log hour
my $current_mm;                        # current log minute
my $current_ss;                        # current log second
my $current_host;                      # current log hostname
my $current_subSysName;                # current log subSystem name
my $current_pid;                       # current log pid
my $current_decSec;                    # current log decimal seconds
my $current_field6_1;                  # current log session
my $current_field6_2;                  # current log transaction
my $current_field6_3;                  # current log thread
my $current_severityLvl;               # current log severity level
my $current_subSysId;                  # current log subSystem id
my $current_msgNum;                    # current log msg number (w/o CAL prefix)
my $current_msgBody;                   # current log message text body

#-------------------------------------------------------------------------------
# Description of hash tables starting with %sessionHashTable.
#
# %sessionHashTable is a hash table of records, where each record carries
# the latest high level data for a host/session.  The session records are
# defined as follows:
#
#   sessionRecord =    # Key is concatenated host and session strings
#   {
#     hostName         # host server
#     sessionId        # session id
#     transactionId    # current transaction id
#     statementId      # current SQL statement id
#     versionId        # current DBRM version id
#     sqlStatement     # SQL statement associated with this statementId
#     startTimeString  # formatted version of startTime + startDecSec
#     endTimeString    # formatted version of endTime   + endDecSec
#     startDecSec      # decimal seconds associated with startTime
#     stepTable        # table of current active job steps
#     maxMemPct        # peak memory usage for ExeMgr during this query
#     totalTempFileCnt # number of temp files created during this query
#     totalTempFileSpace # temp file disk space (in bytes) for this query
#     totalPhysRead    # total no. of physcal blk reads (from db) for this query
#     totalCacheRead   # total no. of cache blk reads (from db) for this query
#     totalMsgsRcvd    # total no. of msgs received from primproc for this query
#     totalMsgBytesIn  # total no. of bytes in incoming messages
#     totalMsgBytesOut # total no. of bytes in outgoing messages
#     totalCPBlks      # total no. of blocks skipped due to casual partitioning
#   }   ||
#       ||
#       \/
# where stepTable is a hash table that carries a list of step records
# pertaining to a list of job steps for a session.  The stepTable records
# are defined as follows:
#
#   stepTableRecord =  # Key is stepId taken from syslog message
#   {
#     stepName         # name or description of a step
#     subSystem        # name of subSystem performing the step (always ExeMgr)
#     startTime        # time when step started to execute (seconds)
#     startDecSec      # decimal seconds associated with startTime
#     endTime          # time when step completed execution (seconds)
#     endDecSec        # decimal seconds associated with endTime
#     effStartTime     # effective time when step starts processing input (secs)
#     effStartDecSec   # decimal seconds associated with effStartTime
#     effEndTime       # effective time when step stops processing output (secs)
#     effEndDecSec     # decimal seconds associated with effEndTime
#     blockedFifoIn    # number of FIFO read operations that blocked
#     blockedFifoOut   # number of FIFO write operations that blocked
#     msgBytesIn       # number of bytes in incoming messages
#     msgBytesOut      # number of bytes in outgoing messages
#     isStepOpen       # boolean flag indicating whether step is completed
#     objectTable      # table of objects related to this step
#   }   ||
#       ||
#       \/
# where objectTable is a hash table carrying a list of object records
# pertaining to the objects relevant to a given step. The objectTable
# records are defined as follows:
#
#   objectTableRecord =# Key is objectId taken from syslog message
#   {
#     physicalRead     # number of blocks physically read from disk for this obj
#     cacheRead        # number of blocks logically read from cache for this obj
#     cpBlks           # number of blocks skipped due to casual partitioning
#   }
# 
#-------------------------------------------------------------------------------
my %sessionHashTable;

#-------------------------------------------------------------------------------
# Print command line usage
#
# input  arguments: none
# return value(s) : none
#-------------------------------------------------------------------------------
sub usage
{
  die "$0: [-h] [-s sessionId] [-d] logfile\n" .
      "  -h       displays this help message\n" .
      "  -s       id of session to be extracted from syslog file\n" .
      "  -d       enables debug mode\n" .
      "  logFile  name of log file to be read\n";
}

#-------------------------------------------------------------------------------
# Parse command line arguments
#
# input  arguments: none
# return value(s) : none
#-------------------------------------------------------------------------------
sub parseCmdLineArgs
{
  my $argc = @ARGV;

  foreach (my $i=0; $i<$argc; $i+=1)
  {
    if ( $ARGV[$i] eq "-h" )
    {
      usage();
    }
    elsif ( $ARGV[$i] eq "-s" )
    {
      $i+=1;
      $sessionToExtract = $ARGV[$i];
    }
    elsif ( $ARGV[$i] eq "-d" )
    {
      $debugFlag = 1;
    }
    elsif ( ($i+1) == $argc )
    {
      $logFileName = $ARGV[$i];
    }
  }

  if ( $logFileName eq "" )
  {
    print STDERR "\nname of log file must be provided on command line\n\n";
    usage();
  }
  if ( $debugFlag == 1 )
  {
    print STDERR "Running in debug mode...\n";
  }
  if ( $sessionToExtract != $SESSION_ID_WILDCARD )
  {
    print STDERR "Limiting report to sessionId ", $sessionToExtract, "\n";
  }
}

#-------------------------------------------------------------------------------
# Determine current year so that we have a year to use in constructing
# time stamps.  The year is not present in the input log file.
# We will assume it is the current year.  It doesn't make that much
# difference what year we use, because we are only using it to create
# the time stamps we use in calculating elapsed times.  But we'll use
# the current year to try to be correct.
#
# input  arguments: none
# return value(s) : string representing current year
#-------------------------------------------------------------------------------
sub getCurrentYear
{
  my ($currSec, $currMin, $currHour,
      $currMday,$currMon, $currYear,
      $currWday,$currWyr, $currDst) = localtime();

  return $currYear;
}

#-------------------------------------------------------------------------------
# Create a time stamp (in seconds) using the date/time associated with the
# current log record.  Decimal seconds field from the log record is incorporated
# into the time stamp.
#
# input  arguments: none
# return value(s) : returns time stamp (in seconds) taken from the latest
#                   log record we have read.
#-------------------------------------------------------------------------------
sub createCurrentTimeStamp
{
  my $monNum    = $MON_ALPHA_TO_NUM{$current_monAlpha};
  my $timeStamp = timelocal($current_ss,
                            $current_mm,
                            $current_hh,
                            $current_mday,
                            $monNum,
                            $timeStampYear);

  # If the decimal seconds field does not match the initial date/time stamp
  # of the log record, we overwrite the seconds portion of the date/time stamp.
  # Before doing that we first see if a minute wrap-around has occurred, in
  # which case we first subtract 60 seconds before substituting the seconds
  # from the decimal seconds field into the time stamp.
  my ($wholeDecSec,$fracDecSec)  = ($current_decSec =~ /([\d]+)\.([\d\D]+)/);
  if ($current_ss != $wholeDecSec)
  {
    if (($current_ss < 5) && ($wholeDecSec > 55))
    {
      $timeStamp = $timeStamp - 60;
    }
    $timeStamp = $timeStamp - $current_ss + $wholeDecSec;
  }

  return $timeStamp;
}

#-------------------------------------------------------------------------------
# Given a start date/time stamp and accompanying decimal seconds, and
# a second end date/time stamp and accompanying decimal seconds, this
# subroutine will return the difference between the 2 times.  Subroutine
# assumes the decimal seconds is a string carrying 6 digits of precision,
# with leading zeros.
#
# input  arguments: starting time (seconds),
#                   starting time (decimal seconds),
#                   end time      (seconds),
#                   end time      (decimal seconds)
# return value(s) : delta seconds and delta decimal seconds
#-------------------------------------------------------------------------------
sub deltaTwoTimes
{
  my ($startTime, $startDecSec, $endTime, $endDecSec) = @_;

  my $deltaTime   = $endTime   - $startTime;
  my $deltaDecSec = $endDecSec - $startDecSec;

  if ($deltaDecSec < 0)
  {
    $deltaTime   -= 1;
    $deltaDecSec += 1000000;
  }

  #
  # Round off decimal seconds to 3 digits
  #
  my $deltaDecSecTruncated = int( $deltaDecSec / 1000 );
  my $deltaDecSecRounded   = int( ($deltaDecSec + 500) / 1000);
  if ( $deltaDecSecRounded == 1000 )
  {
    $deltaDecSecRounded  = 0;
    $deltaTime          += 1;
  }
  $deltaDecSec = $deltaDecSecRounded;

  return ($deltaTime, $deltaDecSec);
}

#-------------------------------------------------------------------------------
# Format a time interval in seconds, into an equivalent time interval as
# an hh:mm:ss string.
#
# input  arguments: time interval in seconds
# return value(s) : string formatted as "hh:mm:ss"
#-------------------------------------------------------------------------------
sub formatTimeInterval
{
  my ($timeInterval) = @_;

  my $hr  = 0;
  my $min = 0;
  my $sec = 0;

  if ( $timeInterval > 0 )
  {
    $hr           =  int ($timeInterval / 3600);
    $timeInterval =  $timeInterval - ($hr * 3600);
    if ($timeInterval > 0)
    {
      $min = int ($timeInterval / 60);
      $sec = $timeInterval - ($min * 60);
    }
  }

  return ( sprintf("%02d:%02d:%02d", $hr, $min, $sec) );
}

#-------------------------------------------------------------------------------
# Dump contents of our session hash table.
#
# input  arguments: none
# return value(s) : none
#-------------------------------------------------------------------------------
sub dumpSessionHashTable
{
  my $sessionHashKey;
  my $stepHashKey;
  my $objectHashKey;

  print STDERR "Dump of Session Hash Table...\n";

  #
  # Loop through collection of sessions
  #
  for $sessionHashKey ( sort keys %sessionHashTable )
  {
    if ( $sessionHashTable{$sessionHashKey}->{statementId} == -1 )
    {
      print STDERR
        "Host: "        , $sessionHashTable{$sessionHashKey}->{hostName} , "; ",
        "Session: "     , $sessionHashTable{$sessionHashKey}->{sessionId}, "; ",
        "\n" ,
        "  No active statements for this host/session..." ,
        "\n";
    }
    else
    {
      print STDERR
        "Host: "        , $sessionHashTable{$sessionHashKey}->{hostName} , "; ",
        "Session: "     , $sessionHashTable{$sessionHashKey}->{sessionId}, "; ",
        "Transaction: " , $sessionHashTable{$sessionHashKey}->{transactionId};

      print STDERR
        "; StatementId: " , $sessionHashTable{$sessionHashKey}->{statementId},
        "; StartTime: " , $sessionHashTable{$sessionHashKey}->{startTimeString},
        "; StartDecSec: " , $sessionHashTable{$sessionHashKey}->{startDecSec},
        "\n";

      print STDERR
        "MaxMemPct: "   , $sessionHashTable{$sessionHashKey}->{maxMemPct}, "; ",
        "TotalTempFileCnt: ",
                   $sessionHashTable{$sessionHashKey}->{totalTempFileCnt}, "; ",
        "TotalTempFileSpace: ",
                 $sessionHashTable{$sessionHashKey}->{totalTempFileSpace},
        "\n",
        "TotalPhysRead: ",
                      $sessionHashTable{$sessionHashKey}->{totalPhysRead}, "; ",
        "TotalCacheRead:",
                     $sessionHashTable{$sessionHashKey}->{totalCacheRead}, "; ",
        "TotalCPBlocks:",
                      $sessionHashTable{$sessionHashKey}->{totalCPBlks}, "; ",
        "TotalMsgsRcvd:",
                      $sessionHashTable{$sessionHashKey}->{totalMsgsRcvd},
        "\n";

      my $stepHashTable = $sessionHashTable{$sessionHashKey}->{stepTable};
      print STDERR "  Number of job steps: ";
      print STDERR scalar( keys %$stepHashTable );
      print STDERR "\n";

      #
      # Loop through collection of steps for each session
      #
      for $stepHashKey ( sort { $a <=> $b } keys %$stepHashTable )
      {
        my $jobStep       = $stepHashTable->{$stepHashKey};
        my $formattedTime = asctime( localtime($jobStep->{startTime}) );
        chomp ( $formattedTime );
        print STDERR
          "    Id: "          , $stepHashKey ,
          "; Name: "          , $jobStep->{stepName} ,
          "; SubSystem: "     , $jobStep->{subSystem} ,
          "; StartTime: "     , $formattedTime ,
          "; StartDecSec: "   , $jobStep->{startDecSec} ,
          "; IsStepOpen: "    , $jobStep->{isStepOpen} ,
          "; BlockedFifoIn: " , $jobStep->{blockedFifoIn} ,
          "; BlockedFifoOut: ", $jobStep->{blockedFifoOut},
          "; MsgBytesIn: "    , $jobStep->{msgBytesIn},
          "; MsgBytesOut: "   , $jobStep->{msgBytesOut},
          "\n";

        my $objectHashTable = $jobStep->{objectTable};
        print STDERR "      Number of objects: ";
        print STDERR scalar( keys %$objectHashTable );
        print STDERR "\n";

        #
        # Loop through collection of objects for each job step
        #
        for $objectHashKey ( keys %$objectHashTable )
        {
          my $object = $objectHashTable->{$objectHashKey};
          print STDERR
            "        Id: "      , $objectHashKey           ,
            "; PhysicalRead: "  , $object->{physicalRead}  ,
            "; CacheRead: "     , $object->{cacheRead}     ,
            "; CPBlks: "        , $object->{cpBlks}        ,
            "\n";
        }
      }
    }

    print STDERR "\n";
  }
}

#-------------------------------------------------------------------------------
# Print a SQL statement to STDOUT that is formatted or broken up into
# substrings, with each substring less than a given maximum line length
# ($SQL_LINE_LENGTH).
#
# input  arguments: SQL statement
# return value(s) : none
#-------------------------------------------------------------------------------
sub printFormattedSql
{
  my ($sql) = @_;

  $Text::Wrap::columns = $SQL_LINE_LENGTH;

  print wrap ( "  ", "  ", ($sql) );
  print "\n";
}

#-------------------------------------------------------------------------------
# Print header info for the subsequent SQL statement in the output report.
#
# input  arguments: $sessionRecord we are going to be writing out to report
# return value(s) : none
#-------------------------------------------------------------------------------
sub printStatementHeaders
{
  my ($sessionRecord) = @_;

  my $hostName        = $sessionRecord->{hostName};
  my $sessionId       = $sessionRecord->{sessionId};
  my $transactionId   = $sessionRecord->{transactionId};
  my $sqlStatement    = $sessionRecord->{sqlStatement};
  my $statementId     = $sessionRecord->{statementId};
  my $startTimeString = $sessionRecord->{startTimeString};
  my $endTimeString   = $sessionRecord->{endTimeString};
  my $versionId       = $sessionRecord->{versionId};

  #
  # Record "Start Statement" to our output report
  #
  print "****************************************" ,
        "****************************************\n";
  print "Host: $hostName   SessionID: $sessionId   " ,
        "TransactionID: $transactionId" ,
        "\n\n";

  printFormattedSql ( $sqlStatement );

  print "  \n" ,
        "  StatementID: $statementId   StartTime: $startTimeString   " ,
        "EndTime: $endTimeString   VersionID: $versionId\n" ,
        "\n\n";

  #
  # Print line 1 of column header
  #
  print "  Step Step                 ",
        "Start            Elapsed       ",
        "Effective     Effective     Blocked  Blocked  ",
        "Input       Output    ";
        for (my $i=0; $i<$NUM_OBJECTS_TO_RPT_PER_LINE; $i++)
        {
          print "  Object  Physical    Cache       CasualPart";
        }
  print "\n";

  #
  # Print line 2 of column header
  #
  print "  ID   Name                 ",
        "Time             Time          ",
        "Start Time    Elapsed Time  FifoIn   FifoOut  ",
        "Msg KB      Msg KB    ";
        for (my $i=0; $i<$NUM_OBJECTS_TO_RPT_PER_LINE; $i++)
        {
          print "  ID      Read        Read        Blocks    ";
        }
  print "\n";

  #
  # Print line 3 of column header
  #
  print "  ---- -------------------- ",
        "---------------  ------------  ",
        "------------  ------------  -------  -------  ",
        "----------  ----------";
        for (my $i=0; $i<$NUM_OBJECTS_TO_RPT_PER_LINE; $i++)
        {
          print "  ------  ----------  ----------  ----------";
        }
  print "\n";
}

#-------------------------------------------------------------------------------
# Print trailer info for the recent SQL statement in the output report.
#
# input  arguments: $sessionRecord we are writing out to report
# return value(s) : none
#-------------------------------------------------------------------------------
sub printStatementTrailers
{
  my ($sessionRecord) = @_;

  my $maxMemPct          = $sessionRecord->{maxMemPct};
  my $totalTempFileCnt   = $sessionRecord->{totalTempFileCnt};
  my $totalTempFileSpace = $sessionRecord->{totalTempFileSpace};
  my $totalPhysRead      = $sessionRecord->{totalPhysRead};
  my $totalCacheRead     = $sessionRecord->{totalCacheRead};
  my $totalMsgsRcvd      = $sessionRecord->{totalMsgsRcvd};
  my $totalMsgBytesIn    = $sessionRecord->{totalMsgBytesIn};
  my $totalMsgBytesOut   = $sessionRecord->{totalMsgBytesOut};
  my $totalCPBlks        = $sessionRecord->{totalCPBlks};

  # round off to nearest MB for the purpose of the report
  my $totalTempFileSpaceMB = 0;
  my $totalMsgBytesInMB    = 0;
  my $totalMsgBytesOutMB   = 0;
  if ($totalTempFileSpace >= 500000)
  {
    $totalTempFileSpaceMB = int (($totalTempFileSpace+500000) / 1000000);
  }
  if ($totalMsgBytesIn >= 500000)
  {
    $totalMsgBytesInMB = int (($totalMsgBytesIn+500000) / 1000000);
  }
  if ($totalMsgBytesOut >= 500000)
  {
    $totalMsgBytesOutMB = int (($totalMsgBytesOut+500000) / 1000000);
  }

  print "  Max Memory Percentage Use for ExeMgr: $maxMemPct %\n";
  print "  Totals for this query:\n",
        "    TotalTempFileCnt: "       , $totalTempFileCnt,
        ";  TotalTempFileSpace: "      , $totalTempFileSpaceMB, "MB\n",
        "    TotalPhysRead(blocks): "  , $totalPhysRead,
        ";  TotalCacheRead(blocks): "  , $totalCacheRead,
        ";  TotalCPBlks(blocks): "     , $totalCPBlks,
        ";  TotalMsgsReceived: "       , $totalMsgsRcvd, "\n",
        "    TotalMsgBytesIn: "        , $totalMsgBytesInMB, "MB",
        ";  TotalMsgBytesOut: "        , $totalMsgBytesOutMB, "MB",
        "\n\n";
}

#-------------------------------------------------------------------------------
# Close and report the stats for all the job steps listed in the stepHashTable
# for the specified sessionRecord and corresponding statement.
#
# input  arguments: $sessionRecord having %stepHashTable with steps to be closed
# return value(s) : 0 - no steps forced to end
#                  >0 - number of open steps that had to be closed
#-------------------------------------------------------------------------------
sub printAndEndAllSteps
{
  my ($sessionRecord) = @_;

  my $hostName      = $sessionRecord->{hostName};
  my $sessionId     = $sessionRecord->{sessionId};
  my $statementId   = $sessionRecord->{statementId};
  my $stepHashTable = $sessionRecord->{stepTable};

  my $stepHashKey;
  my $stillOpenStepCount = 0;
  my $stepCount = scalar( keys %$stepHashTable );

  if ($sessionRecord->{sqlStatement} ne $EMPTY_STRING)
  {
    printStatementHeaders ($sessionRecord);

    if ( $stepCount > 0 )
    {
      if ($debugFlag == 1)
      {
        print STDERR "At log record ", $.,
                     ", $stepCount steps reported as complete for ",
                     "host-$hostName; session-$sessionId; " ,
                     "statement-$statementId\n";
      }

      my $endTimeStamp = createCurrentTimeStamp();
      my ($endDecSec)  = ($current_decSec =~ /\d+\.([\d\D]+)/);

      #
      # Loop through the steps, sorted numerically by step id
      #
      for $stepHashKey ( sort { $a <=> $b } keys %$stepHashTable )
      {
        my $jobStep = $stepHashTable->{$stepHashKey};

        #
        # Format the start times for the report
        #
        my $formattedTime = asctime( localtime($jobStep->{startTime}) );
        chomp ( $formattedTime );
        my ($formattedStartTimeOnly)    =
          ($formattedTime =~ /\w+\s+\d+\s+([\d:]+)/);

        my $effFormattedTime = asctime( localtime($jobStep->{effStartTime}) );
        chomp ( $effFormattedTime );
        my ($effFormattedStartTimeOnly) =
          ($effFormattedTime =~ /\w+\s+\d+\s+([\d:]+)/);

        #
        # Use EndStatement time for EndStep time, "if" we did not encounter an
        # EndStep log record.
        #
        if ($jobStep->{endTime} == 0)
        {
          $jobStep->{endTime}      = $endTimeStamp;
          #jobStep->{endDecSec}    = $endDecSec;
        }
        if ($jobStep->{effEndTime} == 0)
        {
          $jobStep->{effEndTime}   = $endTimeStamp;
          #jobStep->{effEndDecSec} = $endDecSec;
        }

        #
        # Calculate the elapsed times for this step
        #
        my ($elapsedTimeSec, $elapsedTimeDecSec) =
          deltaTwoTimes( $jobStep->{startTime},
                         $jobStep->{startDecSec},
                         $jobStep->{endTime},
                         $jobStep->{endDecSec} );
        my $formattedElapsedTime = formatTimeInterval ( $elapsedTimeSec );

        my ($effElapsedTimeSec, $effElapsedTimeDecSec) =
          deltaTwoTimes( $jobStep->{effStartTime},
                         $jobStep->{effStartDecSec},
                         $jobStep->{effEndTime},
                         $jobStep->{effEndDecSec} );
        my $effFormattedElapsedTime = formatTimeInterval ( $effElapsedTimeSec );

        #
        # Add up the number of steps that are still open
        #
        if ($jobStep->{isStepOpen} == 1)
        {
          $stillOpenStepCount += 1;
        }

        # round off msg byte counts to nearest KB for the purpose of the report
        my $msgBytesInKB  = 0;
        my $msgBytesOutKB = 0;
        if ($jobStep->{msgBytesIn} >= 500)
        {
          $msgBytesInKB  = int (($jobStep->{msgBytesIn} +500) / 1000);
        }
        if ($jobStep->{msgBytesOut} >= 500)
        {
          $msgBytesOutKB = int (($jobStep->{msgBytesOut}+500) / 1000);
        }

        #
        # print the performance statistics for this step
        #
        printf "  %04s %-20s %8s.%06d  %8s.%03d  %8s.%03d  %8s.%03d" .
               "  %7d  %7d  %10d  %10d",
          $stepHashKey,
          $jobStep->{stepName},
          $formattedStartTimeOnly,
          $jobStep->{startDecSec},
          $formattedElapsedTime,
          $elapsedTimeDecSec,
          $effFormattedStartTimeOnly,
          $jobStep->{effStartDecSec},
          $effFormattedElapsedTime,
          $effElapsedTimeDecSec,
          $jobStep->{blockedFifoIn},
          $jobStep->{blockedFifoOut},
          $msgBytesInKB,
          $msgBytesOutKB;

        my $objectHashTable = $jobStep->{objectTable};
        my $objectHashKey;
        my $objectCount = 0;

        #
        # Loop through the collection of objects for this step
        #
        for $objectHashKey ( keys %$objectHashTable )
        {
          # there should hopefully be no more than 2 objects involved in
          # a step, so we think it is okay to only report the first 2.
          # $NUM_OBJECTS_TO_REPORT can be changed if we decide we need
          # and want to report on more than 2 objects.
          if ($objectCount < $NUM_OBJECTS_TO_REPORT)
          {
            my $object = $objectHashTable->{$objectHashKey};

            if ($objectCount > 0)
            {
              print "\n";
              printf "                                                  " .
                     "                                                  " .
                     "                                ";
            }
            printf "  %6d  %10d  %10d  %10d",
              $objectHashKey,
              $object->{physicalRead},
              $object->{cacheRead},
              $object->{cpBlks};
          }
          else
          {
            last;      # stop reporting after we reach $NUM_OBJECTS_TO_REPORT
          }

          $objectCount += 1;
        }
        print "\n";

        #
        # Delete contents of the obj hash table by initializing to an empty list
        #
        %$objectHashTable = ();
      }

      #
      # Delete contents of the step hash table by initializing to an empty list
      #
      %$stepHashTable = ();
    }

    #
    # print a blank line after the last job step
    #
    print "\n";

    #
    # print statement summary totals
    #
    printStatementTrailers ($sessionRecord);
  }

  return $stillOpenStepCount;
}

#-------------------------------------------------------------------------------
# Create hash key for the session hash table.
# Key consists of the host name concatenated with the session id.
#
# input  arguments: host string
#                   session string
# return value(s) : concatenated string to be used as session hash table key
#-------------------------------------------------------------------------------
sub createSessionHashTableKey
{
  my ($hostKey, $sessionKey) = @_;
  my $sessionHashKey = $hostKey . $KEY_CONCATENATE_CHAR . $sessionKey;
  
  return $sessionHashKey;
} 

#-------------------------------------------------------------------------------
# Create and insert record to the session hash table.
#
# input  arguments: none
# return value(s) : none
#-------------------------------------------------------------------------------
sub createSessionHashTableRecord
{
  #
  # Create an entry in our sessionHashTable for this session/transaction;
  # also create an empty stepHashTable for later use
  #
  my $hostName      = $current_host;
  my $sessionId     = $current_field6_1;
  my $transactionId = $current_field6_2;

  my $hashKey = createSessionHashTableKey ( $sessionId, $hostName );
  my %stepHashTable;
  my $sessionRecord =
  {
    hostName           => $hostName,
    sessionId          => $sessionId,
    transactionId      => $transactionId,
    statementId        => -1,
    versionId          => -1,
    sqlStatement       => $EMPTY_STRING,
    startTimeString    => $EMPTY_STRING,
    endTimeString      => $EMPTY_STRING,
    stepTable          => \%stepHashTable, #add hash tbl ref to track job steps
    maxMemPct          => 0,
    totalTempFileCnt   => 0,
    totalTempFileSpace => 0,
    totalPhysRead      => 0,
    totalCacheRead     => 0,
    totalMsgsRcvd      => 0,
    totalMsgBytesIn    => 0,
    totalMsgBytesOut   => 0,
    totalCPBlks        => 0

    # The following sessionRecord fields are filled in later as we
    # receive each Start Statement entry for this transaction.
    #   statementId
    #   versionId
    #   sqlStatement
    #   startTimeString
    #   endTimeString
    #   startDecSec
  };

  if ($debugFlag == 1)
  {
    print STDERR "At log record ", $.,
                 ", Create session record for ",
                 "host-$hostName; session-$sessionId\n";
  }

  $sessionHashTable{$hashKey} = $sessionRecord;
}

#-------------------------------------------------------------------------------
# Emulate an End Transaction log record so that we can start a new
# transaction.
#
# input  arguments: none
# return value(s) : none
#-------------------------------------------------------------------------------
sub emulateEndTrans
{
  emulateEndStatement ( 1 );
}

#-------------------------------------------------------------------------------
# Emulate an End Statement log record so that we can start a new statement.
#
# input  arguments: flag indicating whether to treat this as the end of a
#                   transaction as well.
#                   0 - this is not the end of the transaction
#                   1 - this is the end of the transaction
# return value(s) : none
#-------------------------------------------------------------------------------
sub emulateEndStatement
{
  my ($endOfTransaction) = @_;

  my $hostName      = $current_host;
  my $sessionId     = $current_field6_1;
  my $transactionId = $current_field6_2;

  my $hashKey = createSessionHashTableKey ( $sessionId, $hostName );

  if ( exists($sessionHashTable{$hashKey}) ) # find sessionHashTable entry
  {
    my $sessionRecord = $sessionHashTable{$hashKey};

    my $numberOfStepsClosed = printAndEndAllSteps ($sessionRecord);
    if ( $numberOfStepsClosed > 0 )
    {
      my $statementId = $sessionRecord->{statementId};
      warn ("WARNING: record #", $., ": ",
            "Forced to close $numberOfStepsClosed step(s) without " ,
            "ever receiving an EndStep log message for host: $hostName, " ,
            "session: $sessionId, transaction: $transactionId, " ,
            "statement: $statementId\n");
    }

    #
    # Set statementId to -1 to indicate that we don't currently have an active
    # statement.
    # Do the same to transactionId to denote the end of the transaction if
    # applicable.
    #
    $sessionRecord->{statementId}     = -1;
    $sessionRecord->{versionId}       = -1;
    $sessionRecord->{sqlStatement}    = $EMPTY_STRING;
    $sessionRecord->{startTimeString} = $EMPTY_STRING;
    $sessionRecord->{endTimeString}   = $EMPTY_STRING;

    if ( $endOfTransaction == 1 )
    {
      $sessionRecord->{transactionId} = -1;
    }
  }
}

#-------------------------------------------------------------------------------
# Validate that the specified transaction ID and statement ID (from a log
# record) match those of the specified sessionRecord.
#
# input  arguments: session record to be used in comparison
#                   transactionId of latest log record
#                   statementId  of latest log record
#                   statement type label to use in error message
# return value(s)   0-ID's match; 1-One or both ID's do not match
#-------------------------------------------------------------------------------
sub validateTransactionAndStatement
{
  my ($sessionRecord, $transactionId, $statementId, $statementType) = @_;

  if ($sessionRecord->{transactionId} != $transactionId)
  {
    warn ("ERROR: record #", $., ": ",
          "Transaction id ($transactionId) for a " ,
          "$statementType record does not match " ,
          "the current transactionId ($sessionRecord->{transactionId} " ,
          "for session $sessionRecord->{sessionId})\n");
    return 1;
  }

  if ($sessionRecord->{statementId} != $statementId)
  {
    warn ("ERROR: record #", $., ": ",
          "Statement id ($statementId) for a " ,
          "$statementType record does not match " ,
          "the current statementId ($sessionRecord->{statementId}) " ,
          "for session $sessionRecord->{sessionId})\n");
    return 1;
  }

  return 0;
}

#-------------------------------------------------------------------------------
# Process a Start Transaction log record.
#
# input  arguments: none
# return value(s) : 0-success, 1-parsing error
#-------------------------------------------------------------------------------
sub processStartTrans
{
  #
  # If for some reason, this session has an open transaction pending, we
  # should close the transaction now, so that we can open a new transaction
  #
  emulateEndTrans ();

  #
  # Create necessary sessionHashTable record to track this transaction.
  #
  createSessionHashTableRecord ();

  return 0;
}

#-------------------------------------------------------------------------------
# Process an End Transaction log record.
#
# input  arguments: none
# return value(s) : 0-success, 1-parsing error
#-------------------------------------------------------------------------------
sub processEndTrans
{
  my $hostName      = $current_host;
  my $sessionId     = $current_field6_1;
  my $transactionId = $current_field6_2;

  my $hashKey = createSessionHashTableKey ( $sessionId, $hostName );

  if ( exists($sessionHashTable{$hashKey}) ) # find sessionHashTable entry
  {
    my $sessionRecord = $sessionHashTable{$hashKey};

    if ($sessionRecord->{transactionId} != $transactionId)
    {
      warn ("ERROR: record #", $., ": ",
            "Transaction id ($transactionId) for an " ,
            "EndTransaction does not match " ,
            "the current transaction ($sessionRecord->{transactionId})\n");
      return 1;
    }

    #
    # If for some reason, this session has an open statement pending, we
    # should close the statement now, so that we can end the transaction
    #
    emulateEndStatement ( 1 );
  }
  else
  {
    #
    # Received End Transaction for an unopened session, but this could be
    # valid if we read a log file that starts in the middle of a session;
    # so we only post the warning if debug is enabled.
    #
    if ($debugFlag == 1)
    {
      warn ("WARNING: record #", $., ": ",
            "Received EndTransaction with transactionId $transactionId, " ,
            "for a host and session ($hostName, $sessionId) that is not open",
            "\n");
    }
  }

  return 0;
}

#-------------------------------------------------------------------------------
# Process a Start Statement log record.
#
# input  arguments: none
# return value(s) : 0-success, 1-parsing error
#-------------------------------------------------------------------------------
sub processStartStatement
{
  #
  # If for some reason, this session has an open statement pending, we
  # should close the statement now, so that we can open a new statement
  #
  emulateEndStatement ( 0 );

  my $hostName      = $current_host;
  my $sessionId     = $current_field6_1;
  my $transactionId = $current_field6_2;
  my $dateTimeString= $current_monAlpha . " "  .
                      $current_mday     . " "  .
                      $current_hh       . ":"  .
                      $current_mm       . ":"  .
                      $current_ss       . "("  .
                      $current_decSec   . ")";

  my $statementId;
  my $versionId;
  my $sqlStatement;

  #
  # Parse the message body associated with this message
  #
  if ( (($statementId,
         $versionId,
         $sqlStatement) =
    ($current_msgBody =~ /Statement-(\d+)\s+Ver-(\w+)\s+SQL-([\d\D]+)/)) != 3)
  {
    warn ("ERROR: record #", $., ": ",
          "Unable to parse statementId, versionId, and SQL\n");
    return 1;
  }

  my $hashKey = createSessionHashTableKey ( $sessionId, $hostName );
  my $sessionRecord;

  #
  # Save/track the current statement id and it's start time in our
  # session entry
  #
  if ( exists($sessionHashTable{$hashKey}) ) # find sessionHashTable entry
  {
    $sessionRecord = $sessionHashTable{$hashKey};

    if ($sessionRecord->{transactionId} != $transactionId)
    {
      #
      # If we should make Start and End Transaction log messages mandatory,
      # then this would be an error condition, so we would error out like so:
      #
      #   warn ("ERROR: record #", $., ": ",
      #         "Transaction id ($transactionId) for a " ,
      #         "StartStatement does not match " ,
      #         "the current transaction ($sessionRecord->{transactionId})\n");
      #   return 1;
      #
      # But for now, Start and End Transaction are not necssary, so we just
      # update the transaction id for this new statement, and continue.
      #
      $sessionRecord->{transactionId} = $transactionId;
    }
  }
  else
  {
    #
    # Received Start Statement for an unopened session, but since we are not
    # requiring Start Transaction log entries, this is to be expected for the
    # first statement in each session.  Even if we were requiring Start Trans-
    # action Statements this could still be a valid condition if we are read-
    # ing a log file that starts in the middle of a session.  In that case we
    # still might want to post a warning like so, but we would still continue.
    #
    #  if ($debugFlag == 1)
    #  {
    #     warn ("WARNING: record #", $., ": ",
    #        "Received StartStatement with statementId $statementId, " ,
    #        "for a host and session ($hostName, $sessionId) that is not open",
    #        "\n");
    #  }

    #
    # Create necessary sessionHashTable record to track this transaction.
    #
    createSessionHashTableRecord ();
  }

  $sessionRecord = $sessionHashTable{$hashKey};
  $sessionRecord->{statementId}        = $statementId;
  $sessionRecord->{versionId}          = $versionId;
  $sessionRecord->{sqlStatement}       = $sqlStatement;
  $sessionRecord->{startTimeString}    = $dateTimeString;
  ($sessionRecord->{startDecSec})      = ($current_decSec =~ /\d+\.([\d\D]+)/);
  $sessionRecord->{maxMemPct}          = 0;
  $sessionRecord->{totalTempFileCnti}  = 0;
  $sessionRecord->{totalTempFileSpace} = 0;
  $sessionRecord->{totalPhysRead}      = 0;
  $sessionRecord->{totalCacheRead}     = 0;
  $sessionRecord->{totalMsgsRcvd}      = 0;
  $sessionRecord->{totalMsgBytesIn}    = 0;
  $sessionRecord->{totalMsgBytesOut}   = 0;
  $sessionRecord->{totalCPBlks}        = 0;

  if ($debugFlag == 1)
  {
    print STDERR "At log record ", $.,
                 ", Open a new statement for processing for ",
                 "host-$hostName; session-$sessionId; " ,
                 "statement-$statementId\n";
  }

  return 0;
}

#-------------------------------------------------------------------------------
# Process an End Statement log record.
#
# input  arguments: none
# return value(s) : 0-success, 1-parsing error
#-------------------------------------------------------------------------------
sub processEndStatement
{
  my $hostName      = $current_host;
  my $sessionId     = $current_field6_1;
  my $transactionId = $current_field6_2;
  my $dateTimeString= $current_monAlpha . " "  .
                      $current_mday     . " "  .
                      $current_hh       . ":"  .
                      $current_mm       . ":"  .
                      $current_ss       . "("  .
                      $current_decSec   . ")";

  my $statementId;

  #
  # Parse the message body associated with this message
  #
  if ( (($statementId) =
    ($current_msgBody =~ /Statement-(\d+)/)) != 1)
  {
    warn ("ERROR: record #", $., ": ",
          " Unable to parse statementId\n");
    return 1;
  }

  my $hashKey = createSessionHashTableKey ( $sessionId, $hostName );

  if ( exists($sessionHashTable{$hashKey}) ) # find sessionHashTable entry
  {
    my $sessionRecord = $sessionHashTable{$hashKey};

    #
    # Validate that the transaction and statement ID's match the ones for
    # the currently applicable sessionRecord.
    #
    if (validateTransactionAndStatement ($sessionRecord,
                                         $transactionId,
                                         $statementId,
                                         "EndStatement") != 0)
    {
      return 1;
    }

    #
    # Print all step information for this statement; and if for some reason,
    # this session has any open steps pending, we report that as well.
    #
    $sessionRecord->{endTimeString} = $dateTimeString;
    my $numberOfStepsClosed = printAndEndAllSteps ($sessionRecord);
    if ( $numberOfStepsClosed > 0 )
    {
      my $statementId = $sessionRecord->{statementId};
      warn ("WARNING: record #", $., ": ",
            "Forced to close $numberOfStepsClosed step(s) without " ,
            "ever receiving an EndStep log message for host: $hostName, " ,
            "session: $sessionId, transaction: $transactionId, " ,
            "statement: $statementId\n");
    }

    #
    # Set statementId to -1 to indicate that we don't currently have an active
    # statement.
    #
    $sessionRecord->{statementId}     = -1;
    $sessionRecord->{versionId}       = -1;
    $sessionRecord->{sqlStatement}    = $EMPTY_STRING;
    $sessionRecord->{startTimeString} = $EMPTY_STRING;
    $sessionRecord->{endTimeString}   = $EMPTY_STRING;
  }
  else
  {
    #
    # Received End Statement for an unopened session, but this could be
    # valid if we read a log file that starts in the middle of a session;
    # so we only post the warning if debug is enabled.
    #
    if ($debugFlag == 1)
    {
      warn ("WARNING: record #", $., ": ",
            "Received EndStatement with statementId $statementId, " ,
            "for a host and session ($hostName, $sessionId) that is not open",
            "\n");
    }
  }

  return 0;
}

#-------------------------------------------------------------------------------
# Process a Start Step log record.
#
# input  arguments: none
# return value(s) : 0-success, 1-parsing error
#-------------------------------------------------------------------------------
sub processStartStep
{
  my $hostName      = $current_host;
  my $sessionId     = $current_field6_1;
  my $transactionId = $current_field6_2;

  my $statementId;
  my $stepId;
  my $stepName;

  #
  # Parse the message body associated with this message
  #
  if ( (($statementId,
         $stepId,
         $stepName) =
    ($current_msgBody =~
      /Statement-(\d+)\s+StepId-(\d+)\s+StepName-([\d\D]+)/)) != 3)
  {
    warn ("ERROR: record #", $., ": ",
          "Unable to parse statementId, stepId, and stepName\n");
    return 1;
  }

  my $hashKey = createSessionHashTableKey ( $sessionId, $hostName );

  if ( exists($sessionHashTable{$hashKey}) ) # find sessionHashTable entry
  {
    my $sessionRecord = $sessionHashTable{$hashKey};

    #
    # Validate that the transaction and statement ID's match the ones for
    # the currently applicable sessionRecord.
    #
    if (validateTransactionAndStatement ($sessionRecord,
                                         $transactionId,
                                         $statementId,
                                         "StartStep") != 0)
    {
      return 1;
    }

    #
    # Add this step to the applicable session entry
    #
    my $stepHashTable = $sessionRecord->{stepTable};
    my ($startDecSec) = ($current_decSec =~ /\d+\.([\d\D]+)/);
    my $startTimeStamp= createCurrentTimeStamp();

    my %objectHashTable;
    $stepHashTable->{$stepId} =
    {
      stepName       => $stepName,
      subSystem      => $current_subSysName,
      startTime      => $startTimeStamp,
      startDecSec    => $startDecSec,
      endTime        => 0,
      endDecSec      => 0,
      effStartTime   => $startTimeStamp,
      effStartDecSec => $startDecSec,
      effEndTime     => 0,
      effEndDecSec   => 0,
      isStepOpen     => 1,
      blockedFifoIn  => 0,
      blockedFifoOut => 0,
      msgBytesIn     => 0,
      msgBytesOut    => 0,
      objectTable    => \%objectHashTable #add hash table ref for obj i/o counts
    };
  }
  else
  {
    #
    # Received Start Step for an unopened session, but this could be
    # valid if we read a log file that starts in the middle of a session;
    # so we only post the warning if debug is enabled.
    #
    if ($debugFlag == 1)
    {
      warn ("WARNING: record #", $., ": ",
            "Received StartStep with stepId $stepId, " ,
            "for a host and session ($hostName, $sessionId) that is not open",
            "\n");
    }
  }

  return 0;
}

#-------------------------------------------------------------------------------
# Process an End Step log record.
#
# input  arguments: none
# return value(s) : 0-success, 1-parsing error
#-------------------------------------------------------------------------------
sub processEndStep
{
  my $hostName      = $current_host;
  my $sessionId     = $current_field6_1;
  my $transactionId = $current_field6_2;

  my $statementId;
  my $stepId;
  my $blockedFifoIn;
  my $blockedFifoOut;
  my $msgBytesIn;
  my $msgBytesOut;

  #
  # Parse the message body associated with this message
  #
  if ( (($statementId,
         $stepId,
         $blockedFifoIn,
         $blockedFifoOut,
         $msgBytesIn,
         $msgBytesOut) =
    ($current_msgBody =~
        /Statement-(\d+)\s+StepId-(\d+)\s+BlockedFifoIn-(\d+)\s+BlockedFifoOut-(\d+)\s+MsgBytesIn-(\d+)\s+MsgBytesOut-(\d+)/)) != 6)
  {
    warn ("ERROR: record #", $., ": ",
          "Unable to parse statementId, stepId, blockedFifoIn, ",
          "blockedFifoOut, msgBytesIn, and msgBytesOut\n");
    return 1;
  }

  my $hashKey = createSessionHashTableKey ( $sessionId, $hostName );

  if ( exists($sessionHashTable{$hashKey}) ) # find sessionHashTable entry
  {
    my $sessionRecord = $sessionHashTable{$hashKey};

    #
    # Validate that the transaction and statement ID's match the ones for
    # the currently applicable sessionRecord.
    #
    if (validateTransactionAndStatement ($sessionRecord,
                                         $transactionId,
                                         $statementId,
                                         "EndStep") != 0)
    {
      return 1;
    }

    my $stepHashTable = $sessionRecord->{stepTable};

    #
    # Find and close out this step
    #
    if ( exists($stepHashTable->{$stepId}) )
    {
      my $jobStep = $stepHashTable->{$stepId};

      my ($endDecSec) = ($current_decSec =~ /\d+\.([\d\D]+)/);

      $jobStep->{endTime}        = createCurrentTimeStamp();
      $jobStep->{endDecSec}      = $endDecSec;
      $jobStep->{blockedFifoIn}  = $blockedFifoIn;
      $jobStep->{blockedFifoOut} = $blockedFifoOut;
      $jobStep->{msgBytesIn}     = $msgBytesIn;
      $jobStep->{msgBytesOut}    = $msgBytesOut;

      #
      # If we did not encounter an End Processing time, then use the endStep
      # time for the End Processing time.
      # 
      if ($jobStep->{effEndTime} == 0)
      {
        $jobStep->{effEndTime}  = $jobStep->{endTime};
        $jobStep->{effEndDecSec}= $jobStep->{endDecSec};
      }
      $jobStep->{isStepOpen} = 0;
    }
  }
  else
  {
    #
    # Received End Step for an unopened session, but this could be
    # valid if we read a log file that starts in the middle of a session;
    # so we only post the warning if debug is enabled.
    #
    if ($debugFlag == 1)
    {
      warn ("WARNING: record #", $., ": ",
            "Received EndStep with stepId $stepId, " ,
            "for a host and session ($hostName, $sessionId) that is not open",
            "\n");
    }
  }

  return 0;
}

#-------------------------------------------------------------------------------
# Process a Processing Times log record, containing times that the step began
# reading/writing and finished reading/writing.
#
# input  arguments: none
# return value(s) : 0-success, 1-parsing error
#-------------------------------------------------------------------------------
sub processProcessingTimes
{
  my $hostName      = $current_host;
  my $sessionId     = $current_field6_1;
  my $transactionId = $current_field6_2;

  my $statementId;
  my $stepId;
  my $firstRead_mon;
  my $firstRead_day;
  my $firstRead_yyyy;
  my $firstRead_hh;
  my $firstRead_min;
  my $firstRead_ss;
  my $firstRead_decSec;
  my $lastRead_mon;
  my $lastRead_day;
  my $lastRead_yyyy;
  my $lastRead_hh;
  my $lastRead_min;
  my $lastRead_ss;
  my $lastRead_decSec;
  my $firstWrite_mon;
  my $firstWrite_day;
  my $firstWrite_yyyy;
  my $firstWrite_hh;
  my $firstWrite_min;
  my $firstWrite_ss;
  my $firstWrite_decSec;
  my $lastWrite_mon;
  my $lastWrite_day;
  my $lastWrite_yyyy;
  my $lastWrite_hh;
  my $lastWrite_min;
  my $lastWrite_ss;
  my $lastWrite_decSec;

  #
  # Parse the message body associated with this message
  #
  if ( (($statementId,
         $stepId,
         $firstRead_mon,
         $firstRead_day,
         $firstRead_yyyy,
         $firstRead_hh,
         $firstRead_min,
         $firstRead_ss,
         $firstRead_decSec,
         $lastRead_mon,
         $lastRead_day,
         $lastRead_yyyy,
         $lastRead_hh,
         $lastRead_min,
         $lastRead_ss,
         $lastRead_decSec,
         $firstWrite_mon,
         $firstWrite_day,
         $firstWrite_yyyy,
         $firstWrite_hh,
         $firstWrite_min,
         $firstWrite_ss,
         $firstWrite_decSec,
         $lastWrite_mon,
         $lastWrite_day,
         $lastWrite_yyyy,
         $lastWrite_hh,
         $lastWrite_min,
         $lastWrite_ss,
         $lastWrite_decSec) =
    ($current_msgBody =~ 
        /Statement-(\d+)\s+StepId-(\d+)\s+FirstRead-(\d+)\/(\d+)\/(\d+)\|(\d+):(\d+):(\d+)\.(\d+)\s+LastRead-(\d+)\/(\d+)\/(\d+)\|(\d+):(\d+):(\d+)\.(\d+)\s+FirstWrite-(\d+)\/(\d+)\/(\d+)\|(\d+):(\d+):(\d+)\.(\d+)\s+LastWrite-(\d+)\/(\d+)\/(\d+)\|(\d+):(\d+):(\d+)\.(\d+)/)) != 30)
  {
    warn ("ERROR: record #", $., ": ",
          "Unable to parse statementId, ",
          "stepId, firstRead, lastRead, firstWrite, and lastWrite\n");
    return 1;
  }

  my $hashKey = createSessionHashTableKey ( $sessionId, $hostName );

  if ( exists($sessionHashTable{$hashKey}) ) # find sessionHashTable entry
  {
    my $sessionRecord = $sessionHashTable{$hashKey};

    #
    # Validate that the transaction and statement ID's match the ones for
    # the currently applicable sessionRecord.
    #
    if (validateTransactionAndStatement ($sessionRecord,
                                         $transactionId,
                                         $statementId,
                                         "ProcessingTimesStep") != 0)
    {
      return 1;
    }

    my $stepHashTable = $sessionRecord->{stepTable};

    #
    # Find and set the effective start/end times for this step.
    # In our report, we currently only log the first read and last write,
    # and ignore the last read and first write.
    #
    if ( exists($stepHashTable->{$stepId}) )
    {
      my $jobStep = $stepHashTable->{$stepId};

      my ($effStartDecSec) = ($firstRead_decSec =~ /\d+\.([\d\D]+)/);
      $jobStep->{effStartTime}   = timelocal($firstRead_ss,
                                             $firstRead_min,
                                             $firstRead_hh,
                                             $firstRead_day,
                                            ($firstRead_mon-1),
                                            ($firstRead_yyyy-1900));
      $jobStep->{effStartDecSec} = $firstRead_decSec;

      my ($effEndDecSec) = ($lastWrite_decSec =~ /\d+\.([\d\D]+)/);
      $jobStep->{effEndTime}     = timelocal($lastWrite_ss,
                                             $lastWrite_min,
                                             $lastWrite_hh,
                                             $lastWrite_day,
                                            ($lastWrite_mon-1),
                                            ($lastWrite_yyyy-1900));
      $jobStep->{effEndDecSec}   = $lastWrite_decSec;
    }
  }
  else
  {
    #
    # Received Start Processing Step for an unopened session, but this could be
    # valid if we read a log file that starts in the middle of a session;
    # so we only post the warning if debug is enabled.
    #
    if ($debugFlag == 1)
    {
      warn ("WARNING: record #", $., ": ",
            "Received Start Processing Step for stepId $stepId, " ,
            "for a host and session ($hostName, $sessionId) that is not open",
            "\n");
    }
  }

  return 0;
}

#-------------------------------------------------------------------------------
# Process an I/O log record.
#
# input  arguments: none
# return value(s) : 0-success, 1-parsing error
#-------------------------------------------------------------------------------
sub processIOCount
{
  my $hostName      = $current_host;
  my $sessionId     = $current_field6_1;
  my $transactionId = $current_field6_2;

  my $statementId;
  my $stepId;
  my $objectId;
  my $physicalRead; 
  my $cacheRead; 
  my $cpBlks;

  #
  # Parse the message body associated with this message
  #
  if ( (($statementId,
         $stepId,
         $objectId,
         $physicalRead,
         $cacheRead,
         $cpBlks) =
    ($current_msgBody =~
      /I\/O:\s+Statement-(\d+)\s+StepId-(\d+)\s+ObjectId-(\d+)\s+PhysRead-(\d+)\s+CacheRead-(\d+)\s+CPBlks-(\d+)/)) != 6)
  {
    warn ("ERROR: record #", $., ": ",
          "Unable to parse statement, stepId, objectId, physRead, " ,
          "cacheRead, and cpBlks\n");
    return 1;
  }

  my $hashKey = createSessionHashTableKey ( $sessionId, $hostName );

  if ( exists($sessionHashTable{$hashKey}) ) # find sessionHashTable entry
  {
    my $sessionRecord = $sessionHashTable{$hashKey};

    #
    # Validate that the transaction and statement ID's match the ones for
    # the currently applicable sessionRecord.
    #
    if (validateTransactionAndStatement ($sessionRecord,
                                         $transactionId,
                                         $statementId,
                                         "I/OCount") != 0)
    {
      return 1;
    }

    my $stepHashTable = $sessionRecord->{stepTable};

    #
    # Find and update the object id list and the I/O counts for this step
    #
    if ( exists($stepHashTable->{$stepId}) )
    {
      my $jobStep   = $stepHashTable->{$stepId};
      my $objectHashTable = $jobStep->{objectTable};

      # update or create an object entry for $objectId
      if ( exists($objectHashTable->{$objectId}) )
      {
        my $object = $objectHashTable->{$objectId};
        $object->{physicalRead}   += $physicalRead;
        $object->{cacheRead}      += $cacheRead;
        $object->{cpBlks}         += $cpBlks;
      }
      else
      {
        $objectHashTable->{$objectId} =
        {
          physicalRead   => $physicalRead,
          cacheRead      => $cacheRead,
          cpBlks         => $cpBlks
        };
        my $object = $objectHashTable->{$objectId};
      }
    }
  }
  else
  {
    #
    # Received I/O counts for an unopened session, but this could be
    # valid if we read a log file that starts in the middle of a session;
    # so we only post the warning if debug is enabled.
    #
    if ($debugFlag == 1)
    {
      warn ("WARNING: record #", $., ": ",
            "Received I/O count for stepId $stepId, " ,
            "for a host and session ($hostName, $sessionId) that is not open",
            "\n");
    }
  }

  return 0;
}

#-------------------------------------------------------------------------------
# Process a Statement Summary log record.
#
# input  arguments: none
# return value(s) : 0-success, 1-parsing error
#-------------------------------------------------------------------------------
sub processStatementSummary
{
  my $hostName      = $current_host;
  my $sessionId     = $current_field6_1;
  my $transactionId = $current_field6_2;

  my $statementId;
  my $maxMemPct;
  my $totalTempFileCnt;
  my $totalTempFileSpace;
  my $totalPhysRead;
  my $totalCacheRead;
  my $totalMsgsRcvd;
  my $totalMsgBytesIn;
  my $totalMsgBytesOut;
  my $totalCPBlks;

  #
  # Parse the message body associated with this message
  #
  if ( (($statementId,
         $maxMemPct,
         $totalTempFileCnt,
         $totalTempFileSpace,
         $totalPhysRead,
         $totalCacheRead,
         $totalMsgsRcvd,
         $totalMsgBytesIn,
         $totalMsgBytesOut,
         $totalCPBlks) =
    ($current_msgBody =~
      /Summary:\s+Statement-(\d+)\s+MaxMemPct-(\d+)\s+TempFileCnt-(\d+)\s+TempFileSpace-(\d+)\s+PhysRead-(\d+)\s+CacheRead-(\d+)\s+MsgsRcvd-(\d+)\s+MsgBytesIn-(\d+)\s+MsgBytesOut-(\d+)\s+CPBlks-(\d+)/)) != 10)
  {
    warn ("ERROR: record #", $., ": ",
          "Unable to parse statement, maxMemPct, tempFileCnt, tempFileSpace, ",
          "physRead, cacheRead, msgsRcvd, msgBytesIn, and msgBytesOut\n");
    return 1;
  }

  my $hashKey = createSessionHashTableKey ( $sessionId, $hostName );

  if ( exists($sessionHashTable{$hashKey}) ) # find sessionHashTable entry
  {
    my $sessionRecord = $sessionHashTable{$hashKey};

    #
    # Validate that the transaction and statement ID's match the ones for
    # the currently applicable sessionRecord.
    #
    if (validateTransactionAndStatement ($sessionRecord,
                                         $transactionId,
                                         $statementId,
                                         "StatementSummary") != 0)
    {
      return 1;
    }

    #
    # Set the statement summary totals for this session record
    #   
    $sessionRecord->{maxMemPct}          = $maxMemPct;
    $sessionRecord->{totalTempFileCnt}   = $totalTempFileCnt;
    $sessionRecord->{totalTempFileSpace} = $totalTempFileSpace;
    $sessionRecord->{totalPhysRead}      = $totalPhysRead;
    $sessionRecord->{totalCacheRead}     = $totalCacheRead;
    $sessionRecord->{totalMsgsRcvd}      = $totalMsgsRcvd;
    $sessionRecord->{totalMsgBytesIn}    = $totalMsgBytesIn;
    $sessionRecord->{totalMsgBytesOut}   = $totalMsgBytesOut;
    $sessionRecord->{totalCPBlks}        = $totalCPBlks;
  }
  else
  {
    #
    # Received Statement Summary for an unopened session, but this could be
    # valid if we read a log file that starts in the middle of a session;
    # so we only post the warning if debug is enabled.
    #
    if ($debugFlag == 1)
    {
      warn ("WARNING: record #", $., ": ",
            "Received Statement Summary " ,
            "for a host and session ($hostName, $sessionId) that is not open",
            "\n");
    }
  }

  return 0;
}

#-------------------------------------------------------------------------------
# Process a log record.
#
# input  arguments: none
# return value(s) : 0-success
#                   1-parsing error
#                   2-not a pertinent msg type
#                   3-log record was filtered based on $sessionToExtract setting
#-------------------------------------------------------------------------------
sub processLogRecord
{
  my $returnStatus = 0;

  #
  # Parse the log message
  #
  if ( (($current_monAlpha,
         $current_mday,
         $current_hh,
         $current_mm,
         $current_ss,
         $current_host,
         $current_subSysName,
         $current_pid,
         $current_decSec,
         $current_field6_1,
         $current_field6_2,
         $current_field6_3,
         $current_severityLvl,
         $current_subSysId,
         $current_msgNum,
         $current_msgBody) =
      ($current_logRecord =~ /(\w+)\s+(\d+)\s+(\d+):(\d+):(\d+)\s+(\w+)\s+(\w+)\[(\d+)\]:\s+([\d\.]+)\s+\|(\d+)\|(\d+)\|(\d+)\|\s+(\w)\s+(\d+)\s+CAL(\d+):\s+([\d\D]+)/)) != 16)
  {
    warn ("ERROR: record #", $., ": ",
          "Unable to parse header fields in record\n");
    return 1;
  }

  if ( ( $sessionToExtract == $SESSION_ID_WILDCARD ) ||
       ( $current_field6_1 == $sessionToExtract ) )
  {
    if ( ($current_msgNum == $LOG_MSG_NUM_START_TRANS)     ||
         ($current_msgNum == $LOG_MSG_NUM_END_TRANS)       ||
         ($current_msgNum == $LOG_MSG_NUM_START_STATEMENT) ||
         ($current_msgNum == $LOG_MSG_NUM_END_STATEMENT)   ||
         ($current_msgNum == $LOG_MSG_NUM_START_STEP)      ||
         ($current_msgNum == $LOG_MSG_NUM_STOP_STEP)       ||
         ($current_msgNum == $LOG_MSG_NUM_IO_COUNT)        ||
         ($current_msgNum == $LOG_MSG_NUM_PROC_TIMES)      ||
         ($current_msgNum == $LOG_MSG_NUM_STATEMENT_SUM) )
    {
      #
      # For some serious verbose debugging, this block can be activated
      # to dump the contents of each log message that we parse.
      #
      if ( $debugFlag == 1 )
      {
      # print STDERR "Parsed performance log record contains:\n";
      # print STDERR "  monAlpha   : $current_monAlpha\n";
      # print STDERR "  mday       : $current_mday\n";
      # print STDERR "  hh         : $current_hh\n";
      # print STDERR "  mm         : $current_mm\n";
      # print STDERR "  ss         : $current_ss\n";
      # print STDERR "  host       : $current_host\n";
      # print STDERR "  subSysName : $current_subSysName\n";
      # print STDERR "  pid        : $current_pid\n";
      # print STDERR "  decSec     : $current_decSec\n";
      # print STDERR "  field6_1   : $current_field6_1\n";
      # print STDERR "  field6_2   : $current_field6_2\n";
      # print STDERR "  field6_3   : $current_field6_3\n";
      # print STDERR "  severityLvl: $current_severityLvl\n";
      # print STDERR "  subSysId   : $current_subSysId\n";
      # print STDERR "  msgNum     : $current_msgNum\n";
      # print STDERR "  msgBody    : $current_msgBody\n";
      }

      #
      # Branch to applicable processing subroutine based on message type
      #
      if    ($current_msgNum == $LOG_MSG_NUM_START_TRANS)
      {
        $returnStatus = processStartTrans ();
      }
      elsif ($current_msgNum == $LOG_MSG_NUM_END_TRANS)
      {
        $returnStatus = processEndTrans ();
      }
      elsif ($current_msgNum == $LOG_MSG_NUM_START_STATEMENT)
      {
        $returnStatus = processStartStatement ();
      }
      elsif ($current_msgNum == $LOG_MSG_NUM_END_STATEMENT)
      {
        $returnStatus = processEndStatement ();
      }
      elsif ($current_msgNum == $LOG_MSG_NUM_START_STEP)
      {
        $returnStatus = processStartStep ();
      }
      elsif ($current_msgNum == $LOG_MSG_NUM_STOP_STEP)
      {
        $returnStatus = processEndStep ();
      }
      elsif ($current_msgNum == $LOG_MSG_NUM_IO_COUNT)
      {
        $returnStatus = processIOCount ();
      }
      elsif ($current_msgNum == $LOG_MSG_NUM_PROC_TIMES)
      {
        $returnStatus = processProcessingTimes ();
      }
      elsif ($current_msgNum == $LOG_MSG_NUM_STATEMENT_SUM)
      {
        $returnStatus = processStatementSummary ();
      }
      else
      {
        $returnStatus = 2;
      }
    }
    else
    {
      $returnStatus = 2;
    }
  }
  else
  {
    $returnStatus = 3;
  }

  return $returnStatus;
}

#-------------------------------------------------------------------------------
# Main entry point for this script
#-------------------------------------------------------------------------------

#
# Parse any command line arguments
#
parseCmdLineArgs ();

#
# Set current year (see getCurrentYear() for explanation)
#
$timeStampYear = getCurrentYear ();

#
# Reopen/reassign STDIN to the specified log file
#
if ( !open(STDIN, "<$logFileName") )
{
  print STDERR "\nUnable to open log file $logFileName\n\n";
  usage();
}

#
# Loop through all the log records read from STDIN
#
while ($current_logRecord = <STDIN>)
{
  chomp($current_logRecord);

  my $returnStatus = processLogRecord ();
  if ( $returnStatus == 1 )
  {
    warn ("ERROR: record #", $., ": ",
          "Error processing log record $.: " ,
          "<$current_logRecord>\n");
  }

  #
  # For some serious verbose debugging, this block can be activated
  # to dump the contents of our hash table after each log record is
  # successfully processed.
  #
  elsif ( $returnStatus == 0 )
  {
    if ($debugFlag == 1)
    {
    # print STDERR "...Dumping hash table after record $.\n";
    # dumpSessionHashTable ();
    }
  }
}

#
# Report any steps that are still pending as EOF is reached.
# This may or may not be what the user expected, so we report
# as a warning.
#
if ($debugFlag == 1)
{
  warn ("\n",
        "WARNING: The following hash table dump shows any steps that ",
        "are still pending when EOF was reached.\n");
  dumpSessionHashTable ();
}
