INSERT INTO supplier
(supplier_id, supplier_name)
SELECT account_no, name
FROM customers
WHERE city = 'Newark';

