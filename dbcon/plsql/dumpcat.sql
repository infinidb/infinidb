-- $Id: dumpcat.sql 4298 2008-08-12 14:20:29Z rdempsey $

col SCHEMA format a10
col TABLENAME format a23
col COLNAME format a23
col OBJECTID format 9999999
col DICT format 9999999
col SCALE format 9999
col PREC format 999
col COLLEN format 99999
col POS format 99

select
	SCHEMA, TABLENAME, COLUMNNAME COLNAME, OBJECTID, DICTOBJECTID DICT,
	DECODE(DATATYPE,
		 0, 'BIT',
		 1, 'TINYINT',
		 2, 'CHAR',
		 3, 'SMALLINT',
		 4, 'DECIMAL',
		 5, 'MEDINT',
		 6, 'INT',
		 7, 'FLOAT',
		 8, 'DATE',
		 9, 'BIGINT',
		10, 'DOUBLE',
		11, 'DATETIME',
		12, 'VARCHAR',
		13, 'CLOB',
		14, 'BLOB'
	) DT,
	SCALE, PREC, COLUMNLENGTH COLLEN, COLUMNPOSITION POS
from
	SYSCOLUMN
order by
	SCHEMA, TABLENAME, POS
/
exit
/
