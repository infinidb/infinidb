-- Named check constraint
  CREATE TABLE STAFFz
  ( EMPNUM CHAR(3),
    SALARY DECIMAL(6) constraint salary CHECK (SALARY > 0));
