
  
    

  create  table "warehouse_db"."warehouse_dw_marts"."mart_purchase_orders__dbt_tmp"
  
  
    as
  
  (
    /*
  Mart Model: mart_purchase_orders
  ===================================
  Aggregates purchase order KPIs by status and supplier.
  Powers procurement dashboard in Power BI.

  Author: Lawrence Koomson
*/

with orders as (
    select * from "warehouse_db"."warehouse_dw"."purchase_orders"
),

suppliers as (
    select supplier_id, supplier_name, region, rating
    from "warehouse_db"."warehouse_dw"."suppliers"
),

products as (
    select product_id, product_name, category
    from "warehouse_db"."warehouse_dw"."products"
),

joined as (

    select
        o.po_id,
        o.status,
        o.order_date,
        o.expected_date,
        o.received_date,
        o.quantity_ordered,
        o.unit_cost_ghs,
        o.total_cost_ghs,

        s.supplier_name,
        s.region,
        s.rating                                        as supplier_rating,

        p.product_name,
        p.category,

        case
            when o.received_date is not null and o.expected_date is not null
            then o.received_date - o.expected_date
            else null
        end                                             as delivery_delay_days,

        case
            when o.status = 'RECEIVED'  then 'On Time'
            when o.status = 'CANCELLED' then 'Cancelled'
            when o.expected_date < current_date
                 and o.status not in ('RECEIVED','CANCELLED')
            then 'Overdue'
            else 'In Progress'
        end                                             as delivery_status

    from orders o
    join suppliers s on o.supplier_id = s.supplier_id
    join products  p on o.product_id  = p.product_id

)

select * from joined
order by order_date desc
  );
  