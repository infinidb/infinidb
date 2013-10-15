/*
Creates the stacks table used to track availability / usage of our development stacks.
*/

create database if not exists stacks;
use stacks;

drop table if exists stacks;
drop table if exists stacksLog;
drop table if exists users;

CREATE TABLE `stacks` (
  `stack` varchar(20) DEFAULT NULL,
  `status` varchar(20) DEFAULT NULL,
  `user` varchar(20) DEFAULT NULL,
  `checkedout` datetime DEFAULT NULL,
  `userModule` varchar(20) DEFAULT NULL,
  `notes` varchar(200) default null,
  `version` char(8)
) ENGINE=MyISAM;

insert into stacks values 
('demo',            'Available', null, null, 'srvdemo1',        '1 UM x 3 PMs', null),
('srvswdev11',      'Available', null, null, 'srvswdev11',      '1 UM/PM', null),
('qaftest2',        'Available', null, null, 'qaftest2',        '1 UM/PM', null),
('devint1',         'Available', null, null, 'srvperf7',        '2 UM/PMs', null),
('devint2',         'Available', null, null, 'srvperf2',        '1 UM x 3 PMs (Shared Nothing)', null),
('devint3',         'Available', null, null, 'srvperf3',        '1 UM/PM      (Shared Nothing)', null),
('devsn1',          'Available', null, null, 'srvdevsn1',       '1 UM x 3 PMs (Shared Nothing)', null),
('devsn2',          'Available', null, null, 'srvdevsn5',       '1 UM x 2 PMs (Shared Nothing)', null),
('devsn3',          'Available', null, null, 'srvdevsn8',       '2 UM/PMs     (Shared Nothing)', null),
('srvalpha2',       'Available', null, null, 'srvalpha2',       '1 UM/PM', null),
('alphad03',        'Available', null, null, 'srvalpha4',       '1 UM x 4 PMs', null),
('qperfd01',        'Available', null, null, 'qaftest7',        '1 UM X 5 PMs', null),
('srvprodtest1',    'Available', null, null, 'srvprodtest1',    '1 UM/PM',null, null);

CREATE TABLE stacksLog (
    stack varchar(20),
    user varchar(20),
    checkedOut datetime,
    checkedIn datetime,
    source varchar(20) /* php or commandLine */
) ENGINE=MyISAM;

CREATE TABLE `users` (
  `user` varchar(20) DEFAULT NULL,
  `mailOnCheckouts` tinyint(1) DEFAULT NULL,
  `mailOnCheckins` tinyint(1) DEFAULT NULL
) ENGINE=MyISAM;

insert into users values
('bpaul', false, false),
('chao', false, false),
('dcathey', false, false),
('dhill', false, false),
('pleblanc', false, false),
('rdempsey', false, false),
('wweeks', false, false),
('xlou', false, false),
('zzhu', false, false);

create table testResults (
    test varchar(20),
    stack varchar(20),
    version varchar(10),
    rel varchar(10),
    start datetime,
    stop datetime,
    buildDtm varchar(30),
    total int,
    status varchar(8),
    failedTests varchar(1000)
) engine=MyISAM;
