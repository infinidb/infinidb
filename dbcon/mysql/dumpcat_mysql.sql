-- $Id: dumpcat_mysql.sql 7049 2010-09-14 16:43:13Z rdempsey $
-- 

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
	              when 13 then 'CLOB'
	              when 14 then 'BLOB'
	              end DATATYPE, 
	SCALE, PREC, COLUMNLENGTH COLLEN, COLUMNPOSITION POS, COMPRESSIONTYPE CT
from
	SYSCOLUMN
order by
	`schema`, TABLENAME, POS;
