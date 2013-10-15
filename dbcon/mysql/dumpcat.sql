-- $Id: dumpcat.sql 7049 2010-09-14 16:43:13Z rdempsey $

select
        `schema`, tablename, columnname colname, objectid, dictobjectid dict,
	datatype,
        scale, prec, columnlength collen, columnposition pos, compressiontype ct
from
        syscolumn
order by
        `schema`, tablename, pos;
