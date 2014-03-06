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
 * $Id: calremoveuserpriority.sql 8777 2012-08-01 21:52:34Z zzhu $
 */

DELIMITER $$
CREATE PROCEDURE infinidb_querystats.calRemoveUserPriority(IN host VARCHAR(50), IN usr VARCHAR(50))
LANGUAGE SQL
NOT DETERMINISTIC
MODIFIES SQL DATA
SQL SECURITY INVOKER
COMMENT 'Procedure to remove a given InfiniDB user user_priority'
BEGIN
	delete from infinidb_querystats.user_priority where upper(user_priority.user) = upper(usr) and upper(user_priority.host) = upper(host);
END$$
DELIMITER ; 
