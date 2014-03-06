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
 * $Id: calsetuserpriority.sql 9440 2013-04-24 21:07:42Z chao $
 */

DELIMITER $$
DROP PROCEDURE IF EXISTS infinidb_querystats.calSetUserPriority;
CREATE PROCEDURE infinidb_querystats.calSetUserPriority(IN host VARCHAR(50), IN usr VARCHAR(50), IN pri VARCHAR(10))
LANGUAGE SQL
NOT DETERMINISTIC
MODIFIES SQL DATA
SQL SECURITY INVOKER
COMMENT 'Procedure to set a given InfiniDB user to the given priority'
pri_validation: BEGIN
	IF upper(pri) not in ('HIGH', 'MEDIUM', 'LOW') THEN
		select "Priority can only be set to 'High', 'Medium', or 'Low'" Error;
		leave pri_validation;
	END IF;

	IF INSTR(host, ":") != 0 THEN
		select "Port number cannot be used when setting user priority" Error;
		leave pri_validation;
	END IF;
	
	user_validation: BEGIN
		DECLARE cnt,c INT;
		DECLARE cur_2 CURSOR for select count(*) from mysql.user where upper(user.host)=upper(host) and upper(user.user)=upper(usr);
		DECLARE CONTINUE HANDLER FOR NOT FOUND
		SET c = 1;
		OPEN cur_2;
		SET cnt = 0;
		REPEAT
		FETCH cur_2 into cnt;
		until c = 1
		END REPEAT;
		IF cnt = 0 THEN
			select "User does not exist in MySQL" Error;
			LEAVE user_validation;
		END IF;
	
		BEGIN
			DECLARE a, b INT;
			DECLARE cur_1 CURSOR FOR SELECT count(*) FROM infinidb_querystats.user_priority where upper(user)=upper(usr) and upper(user_priority.host)=upper(host);
			DECLARE CONTINUE HANDLER FOR NOT FOUND
			SET b = 1;
			OPEN cur_1;
			SET a = 0;
			REPEAT
				FETCH cur_1 INTO a;
				UNTIL b = 1
			END REPEAT;
			CLOSE cur_1;
			IF a = 0 THEN
				insert into infinidb_querystats.user_priority values (host, usr, upper(pri));
			ELSE
				update infinidb_querystats.user_priority set priority=upper(pri) where upper(user)=upper(usr) and upper(user_priority.host)=upper(host);
			END IF;
		END;
	END;
END$$
DELIMITER ; 
