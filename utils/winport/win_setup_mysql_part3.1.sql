use calpontsys;
select calonlinealter(
	'alter table systable add (autoincrement int)') as xxx;
alter table systable add (autoincrement int);
update systable set autoincrement=0 where autoincrement is null;

select calonlinealter(
	'alter table syscolumn add (nextvalue bigint)') as xxx;
alter table syscolumn add (nextvalue bigint);
update syscolumn set nextvalue=1 where nextvalue is null;
update syscolumn set autoincrement='n' where autoincrement is null;

