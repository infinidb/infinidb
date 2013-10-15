-- Create table with primary key
  CREATE TABLE TTIME_BASE
  (PK           INTEGER,
   TS           DATETIME,
   PRIMARY KEY (PK),
   CHECK(PK > 32));
