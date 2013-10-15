DELETE FROM TableA
WHERE NOT EXISTS
  ( select *
     from TableB
     where TableA .field1 = TableB.fieldx
     and TableA .field2 = TableB.fieldz );
