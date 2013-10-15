-- $Id: syscatalog.sql 4761 2009-01-20 16:54:55Z bwelch $
-- SYSTABLE
create table systable (tablename varchar(40),
                       schema varchar(40),
                       objectid integer,
                       createdate date,
                       lastupdate date,
                       init integer,
                       next integer,
                       numofrows integer,
                       avgrowlen integer,
                       numofblocks integer);

-- SYSCOLUMN
create table syscolumn (schema varchar(40),
                        tablename varchar(40),
                        columnname varchar(40),
                        objectid integer,
                        dictobjectid integer,
                        listobjectid integer,
                        treeobjectid integer,
                        datatype integer,
                        columnlength integer,
                        columnposition integer,
                        lastupdate date,
                        defaultvalue varchar(8),
                        nullable integer,
                        scale integer,
                        prec integer,
                        autoincrement char(1),
                        distcount integer,
                        nullcount integer,
                        minvalue varchar(40),
                        maxvalue varchar(40));

-- SYSCONSTRAINT
create table sysconstraint (constraintname varchar(40),
                            schema varchar(40),
                            tablename varchar(40),
                            constrainttype char(1),
                            constraintprimitive varchar(4000),
                            constrainttext varchar(4000),
                            constraintstatus varchar(8),
                            indexname varchar(40),
                            referencedtablename varchar(40),
                            referencedschema varchar(40),
                            referencedconstraintname varchar(40));

-- SYSCONSTRAINTCOL
create table sysconstraintcol (schema varchar(40),
                               tablename varchar(40),
                               columnname varchar(40),
                               constraintname varchar(40));

-- SYSINDEX
create table sysindex (schema varchar(40),
                       tablename varchar(40),
                       indexname varchar(40),
                       listobjectid integer,
                       treeobjectid integer,
                       indextype char(1),
                       multicolflag char(1),
                       createdate date,
                       lastupdate date,
                       recordcount integer,
                       treelevel integer,
                       leafcount integer,
                       distinctkeys integer,
                       leafblocks integer,
                       averageleafcountperkey integer,
                       averagedatablockperkey integer,
                       samplesize integer,
                       clusterfactor integer,
                       lastanalysisdate date);

-- SYSINDEXCOL
create table sysindexcol (schema varchar(40),
                          tablename varchar(40),
                          columnname varchar(40),
                          indexname varchar(40),
                          columnposition integer);

