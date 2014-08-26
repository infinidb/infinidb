-- $Id: dumpcat_mysql.sql 7049 2010-09-14 16:43:13Z rdempsey $
-- 

use calpontsys;
select
	`schema`, TABLENAME, COLUMNNAME COLNAME, OBJECTID, DICTOBJECTID DICT, 
	case datatype when 0 then 'BIT' 
	              when 1 then 'TINYINT'
	              when 2 then 'CHAR'
	              when 3 then 'SMALLINT'
	              when 4 then 'DECIMAL'
	              when 5 then 'MEDINT'
	              when 6 then 'INT'
	              when 7 then 'FLOAT'
	              when 8 then 'DATE'
	              when 9 then 'BIGINT'
	              when 10 then 'DOUBLE'
	              when 11 then 'DATETIME'
	              when 12 then 'VARCHAR'
	              when 13 then 'VARBINARY'
	              when 14 then 'CLOB'
	              when 15 then 'BLOB'
	              when 16 then 'UTINYINT'
	              when 17 then 'USMALLINT'
	              when 18 then 'UDECIMAL'
	              when 19 then 'UMEDINT'
	              when 20 then 'UINT'
	              when 21 then 'UFLOAT'
	              when 22 then 'UBIGINT'
	              when 23 then 'UDOUBLE'
	              end DATATYPE, 
	SCALE, PREC, COLUMNLENGTH COLLEN, COLUMNPOSITION POS, COMPRESSIONTYPE CT,
	DEFAULTVALUE DEF, NULLABLE 'NULL', AUTOINCREMENT AI
from
	SYSCOLUMN
order by
	`schema`, TABLENAME, POS;

