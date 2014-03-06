/* Copyright (C) 2014 InfiniDB, Inc.

   This program is free software; you can redistribute it and/or
   modify it under the terms of the GNU General Public License
   as published by the Free Software Foundation; version 2 of
   the License.

   This program is distributed in the hope that it will be useful,
   but WITHOUT ANY WARRANTY; without even the implied warranty of
   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
   GNU General Public License for more details.

   You should have received a copy of the GNU General Public License
   along with this program; if not, write to the Free Software
   Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston,
   MA 02110-1301, USA. */

/*
 * $Id: calshowprocesslist.sql 9736 2013-08-02 18:49:52Z zzhu $
 */

DELIMITER $$
DROP PROCEDURE IF EXISTS infinidb_querystats.calShowProcessList;
CREATE PROCEDURE infinidb_querystats.calShowProcessList()
LANGUAGE SQL
NOT DETERMINISTIC
MODIFIES SQL DATA
SQL SECURITY INVOKER
COMMENT 'Procedure to set a given InfiniDB user to the given priority'
BEGIN
SELECT id SESSION, processlist.user USER, upper(user_priority.priority) PRIORITY, processlist.host `HOST`,
db `DB`, command `COMMAND`, (now() - interval time second) `START TIME`, time `EXEC TIME`, state `STATE`,
info `INFO` FROM information_schema.processlist
LEFT JOIN infinidb_querystats.user_priority ON
(
UPPER(CASE WHEN INSTR(processlist.host, ':') = 0 
     THEN processlist.host 
     ELSE SUBSTR(processlist.host, 1, INSTR(processlist.host, ':')-1 )
     END) = 
UPPER(CASE WHEN INSTR(user_priority.host, ':') = 0 
     THEN user_priority.host 
     ELSE SUBSTR(user_priority.host, 1, INSTR(user_priority.host, ':')-1 )
     END)
 AND
 UPPER(processlist.user) = UPPER(user_priority.user) 
)
WHERE db != 'infinidb_querystats';
END$$
delimiter ;
