set echo on;
set timing on;
Alter session set nls_date_format = 'yyyy-mm-dd hh24:mi:ss';
Alter session set nls_timestamp_format = 'yyyy-mm-dd hh24:mi:ss';
exec calpont.caltraceon(9);
