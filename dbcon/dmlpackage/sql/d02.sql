DELETE FROM supplier
WHERE EXISTS
  ( select customer.name
     from customer
     where customer.customer_id = supplier.supplier_id
     and customer.customer_name = 'IBM' );

