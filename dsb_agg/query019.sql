SELECT i_brand_id AS brand_id, 
       i_brand AS brand, 
       i_manufact_id, 
       i_manufact,
       sum(ss_ext_sales_price) AS ext_price
FROM date_dim, 
     store_sales, 
     item,
     customer,
     customer_address,
     store
WHERE d_date_sk = ss_sold_date_sk
  AND ss_item_sk = i_item_sk
  AND ss_customer_sk = c_customer_sk
  AND c_current_addr_sk = ca_address_sk
  AND ss_store_sk = s_store_sk
  AND i_category = 'Electronics'     
  AND d_year = 2001                  
  AND d_moy = 8                     
  AND substring(ca_zip,1,5) <> substring(s_zip,1,5)
  AND ca_state = 'CA'               
  AND c_birth_month = 6             
  AND ss_wholesale_cost BETWEEN 45 AND 100
GROUP BY i_brand,
         i_brand_id,
         i_manufact_id,
         i_manufact                