-- test column data types
ALTER TABLE calpont.tbl_name ADD COLUMN col_name char(1);
ALTER TABLE tbl_name ADD COLUMN col_name char(2) engine=infinidb;
ALTER TABLE tbl_name ADD COLUMN col_name char(4) engine = infinidb;
ALTER TABLE tbl_name ADD COLUMN col_name char(8);
ALTER TABLE tbl_name ADD COLUMN col_name varchar(50);
ALTER TABLE tbl_name ADD COLUMN col_name bit;
ALTER TABLE tbl_name ADD COLUMN col_name bit(8);
ALTER TABLE tbl_name ADD COLUMN col_name bit(63);
ALTER TABLE tbl_name ADD COLUMN col_name real(5);
ALTER TABLE tbl_name ADD COLUMN col_name real(10,2);
ALTER TABLE tbl_name ADD COLUMN col_name decimal(1);
ALTER TABLE tbl_name ADD COLUMN col_name decimal(2,2);
ALTER TABLE tbl_name ADD COLUMN col_name decimal(5,4);
ALTER TABLE tbl_name ADD COLUMN col_name decimal(10,8);
ALTER TABLE tbl_name ADD COLUMN col_name float(25);
ALTER TABLE tbl_name ADD COLUMN col_name float(25,10);
ALTER TABLE tbl_name ADD COLUMN col_name double;
ALTER TABLE tbl_name ADD COLUMN col_name int;
ALTER TABLE tbl_name ADD COLUMN col_name bigint;
ALTER TABLE tbl_name ADD COLUMN col_name medint;
ALTER TABLE tbl_name ADD COLUMN col_name smallint;
ALTER TABLE tbl_name ADD COLUMN col_name tinyint;
ALTER TABLE tbl_name ADD COLUMN col_name date;
ALTER TABLE tbl_name ADD COLUMN col_name datetime;
ALTER TABLE tbl_name ADD COLUMN col_name clob;
ALTER TABLE tbl_name ADD COLUMN col_name blob;
ALTER TABLE tbl_name ADD COLUMN col_name numeric(7);
ALTER TABLE tbl_name ADD COLUMN col_name numeric(1,1);
ALTER TABLE tbl_name ADD COLUMN col_name numeric(8,1);
ALTER TABLE tbl_name ADD COLUMN col_name numeric(20,10);
ALTER TABLE tbl_name ADD COLUMN col_name number;
ALTER TABLE tbl_name ADD COLUMN col_name integer;
ALTER TABLE tbl_name ADD col_name int;
ALTER TABLE tbl_name ADD col_name CHAR (4);


-- test column constraints
ALTER TABLE calpont.tbl_name ADD COLUMN col_name datetime not null;
ALTER TABLE tbl_name ADD COLUMN col_name float(25,10) null engine=infinidb;

ALTER TABLE tbl_name ADD COLUMN col_name int auto_increment;

ALTER TABLE tbl_name ADD COLUMN col_name decimal(10,2) default 1;
ALTER TABLE tbl_name ADD COLUMN col_name char(1) default 'unknown';
ALTER TABLE tbl_name ADD COLUMN col_name char(1) default USER;
ALTER TABLE tbl_name ADD COLUMN col_name char(1) default CURRENT_USER;
ALTER TABLE tbl_name ADD COLUMN col_name char(1) default SESSION_USER;
ALTER TABLE tbl_name ADD COLUMN col_name char(1) default SYSTEM_USER;
ALTER TABLE tbl_name ADD COLUMN col_name char(1) default NULL;

ALTER TABLE tbl_name ADD COLUMN col_name clob check (col_name < 0),
alter table tbl_name add column col_name int check (col_name > 1)  initially deferred deferrable;
alter table tbl_name add column col_name integer check (col_name = 1)  initially immediate not deferrable;

ALTER TABLE tbl_name ADD COLUMN col_name numeric(5,2) primary key;
ALTER TABLE tbl_name ADD COLUMN col_name numeric(25,2) primary key disabled;
ALTER TABLE tbl_name ADD COLUMN col_name numeric(25) primary key DEFERRED;

ALTER TABLE tbl_name ADD COLUMN col_name int references tbl_name1(col_name);
ALTER TABLE tbl_name ADD COLUMN col_name int references tbl_name1(col_name, col_name1);

ALTER TABLE tbl_name ADD COLUMN col_name int unique;
ALTER TABLE tbl_name ADD COLUMN col_name decimal(8,2) unique DISABLE;
ALTER TABLE tbl_name ADD COLUMN col_name decimal(8) unique DEFERRED;


-- test out-of-line constraints
ALTER TABLE calpont.tbl_name ADD CONSTRAINT const_name check (col_name) (col_name > 0);
alter table tbl_name add check (col_name) (col_name > 0) DEFERRED engine=infinidb;
alter table tbl_name add const_name check (col_name) (col_name > 0) DISABLE;

alter table tbl_name add CONSTRAINT const_name primary key (col_name);
alter table tbl_name add const_name primary key(col_name);
alter table tbl_name add primary key (col_name);
alter table tbl_name add const_name primary key (col_nam,col_name1);

alter table tbl_name add CONSTRAINT const_name unique(col_name);
alter table tbl_name add unique(col_name);
alter table tbl_name add const_name unique col_name DISABLE;
alter table tbl_name add const_name unique col_name DEFERRED;
alter table tbl_name add const_name unique (col_name, col_name1);

ALTER TABLE tbl_name add CONSTRAINT const_name foreign key(col_name) references Customers( p_a );
ALTER TABLE tbl_name ADD FOREIGN KEY(col_name) references Customers( p_a );
alter table tbl_name add foreign key (col_name,col_name1) references Customers( p_a );
alter table tbl_name add foreign key (col_name,col_name1) references Customers( p_a,p_b ) MATCH FULL;
alter table tbl_name add foreign key (col_name,col_name1) references Customers( p_a ) MATCH PARTIAL;
alter table tbl_name add foreign key (col_name,col_name1) references Customers( p_a ) ON UPDATE CASCADE;
alter table tbl_name add foreign key (col_name,col_name1) references Customers( p_a ) ON UPDATE SET NULL;
alter table tbl_name add foreign key (col_name,col_name1) references Customers( p_a ) ON UPDATE SET DEFAULT;
alter table tbl_name add foreign key (col_name,col_name1) references Customers( p_a ) ON UPDATE NO ACTION;
alter table tbl_name add foreign key (col_name,col_name1) references Customers( p_a ) ON DELETE CASCADE;
alter table tbl_name add foreign key (col_name,col_name1) references Customers( p_a ) ON DELETE SET NULL;
alter table tbl_name add foreign key (col_name,col_name1) references Customers( p_a ) ON DELETE SET DEFAULT;
alter table tbl_name add foreign key (col_name,col_name1) references Customers( p_a ) ON DELETE NO ACTION;
alter table tbl_name add foreign key (col_name,col_name1) references Customers( p_a );

ALTER TABLE tbl_name storage(initial 100000 next 200000);


-- test contsraint state
alter table calpont.tbl_name DISABLE constraint const_name;
alter table tbl_name ENABLE constraint const_name engine=infinidb;


-- test alter column
alter table calpont.tbl_name alter col_name drop not null;
alter table tbl_name alter col_name drop null engine=infinidb;
alter table tbl_name alter col_name drop default;
alter table tbl_name alter col_name drop auto_increment;
ALTER TABLE tbl_name ALTER COLUMN col_name DROP DEFAULT;

alter table tbl_name alter col_name set not null;
alter table tbl_name alter col_name set null;
alter table tbl_name alter col_name set default 3;
alter table tbl_name alter col_name set default 'unknown';
alter table tbl_name alter col_name set default USER;
alter table tbl_name alter col_name set default CURRENT_USER;
alter table tbl_name alter col_name set default SESSION_USER;
alter table tbl_name alter col_name set default SYSTEM_USER;
alter table tbl_name alter col_name set default NULL;
alter table tbl_name alter col_name set auto_increment;
ALTER TABLE tbl_name ALTER COLUMN col_name SET DEFAULT;


-- test drop column
alter table calpont.tbl_name drop col_name engine=infinidb;
ALTER TABLE tbl_name DROP col_name CASCADE;
ALTER TABLE tbl_name DROP col_name RESTRICT;
ALTER TABLE tbl_name DROP col_name INVALIDATE;
ALTER TABLE tbl_name DROP col_name CASCADE CONSTRAINTS;
ALTER TABLE tbl_name DROP ( col_name , col_name1) CASCADE CONSTRAINTS;
alter table tbl_name drop column col_name;


-- test rename column and table
alter table calpont.tbl_name rename col_name to col_name1;
alter table tbl_name rename column col_name to col_name1 engine=infinidb;
ALTER TABLE tbl_name RENAME COLUMN col_name to col_name1;
ALTER TABLE tbl_name RENAME to new_tbl_name ;


-- test set column
alter table tbl_name set unused col_name;
alter table tbl_name set unused (col_name,col_name1);


-- test MAX name sizes
ALTER TABLE tbl_name0123456789012345678901234567890 ADD COLUMN col_name char(1);
ALTER TABLE tbl_name ADD COLUMN col_name90123456789012345678901234567890 char(1);
ALTER TABLE schema7890123456789012345678901234567890.tbl_name ADD COLUMN col_name char(1);
ALTER TABLE tbl_name ADD CONSTRAINT const_name123456789012345678901234567890 CHECK(col_name > 1);
ALTER TABLE schema7890123456789012345678901234567890.tbl_name0123456789012345678901234567890 ADD CONSTRAINT const_name123456789012345678901234567890 CHECK(col_name90123456789012345678901234567890 > 1);


-- test combination request
ALTER TABLE tbl_name ADD (col_name int, col_name1 timestamp, col_name2 NUMERIC(7));
ALTER TABLE tbl_name ADD COLUMN col_name int null references Customers(col_name);
ALTER TABLE tbl_name ADD COLUMN col_name int NOT null auto_increment default 1000 references tbl_name1(col_name);
ALTER TABLE tbl_name ADD COLUMN col_name clob not null check (col_name < 0),
ALTER TABLE tbl_name ADD COLUMN col_name numeric(25,2) primary key unique check (col_name = 50);
ALTER TABLE tbl_name ADD COLUMN col_name numeric(25,2) primary key unique check (col_name = 50) disable references tbl_name2(col_name1) DEFERRED;
ALTER TABLE tbl_name ADD COLUMN col_name real(10,2) ADD CONSTRAINT const_name (p_partkey) (p_partkey > 0);
ALTER TABLE tbl_name ADD COLUMN col_name float(10,2) NOT null auto_increment default 1000 primary key DEFERRED unique DISABLED check (col_name = 50) references tbl_name1(col_name) engine=infinidb;


-- test big do all test
ALTER TABLE calpont.tbl_name 
	ADD (
	col_name float(10,2) NOT null auto_increment default 1000 primary key DEFERRED unique DISABLED check (col_name = 50) references tbl_name1(col_name), 
	col_name1 timestamp , 
	col_name2 NUMERIC(7)
	)
	ADD CONSTRAINT const_name check (col_name2) (col_name2 > 0)
	add CONSTRAINT const_name1 primary key (col_name2)
	add CONSTRAINT const_name2 unique(col_name1)
	add CONSTRAINT const_name3 foreign key(col_name) references Customers( p_a )
	storage(initial 100000 next 200000)
	DISABLE constraint const_name
	alter col_name drop not null
	alter table tbl_name alter col_name2 set default 'unknown'
	DROP ( col_name , col_name1) CASCADE CONSTRAINTS
	RENAME COLUMN col_name to col_name1;
	RENAME to new_tbl_name
	alter table tbl_name set unused (col_name,col_name1)
	engine=infinidb;

