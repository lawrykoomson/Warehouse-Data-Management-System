
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select stock_value_ghs
from "warehouse_db"."warehouse_dw_staging"."stg_inventory"
where stock_value_ghs is null



  
  
      
    ) dbt_internal_test