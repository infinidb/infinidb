UPDATE supplier  
SET supplier_name='joe',supplier_state='ca'
WHERE EXISTS
  ( SELECT customer.name
    FROM customers
    WHERE customers.customer_id = supplier.supplier_id); 

