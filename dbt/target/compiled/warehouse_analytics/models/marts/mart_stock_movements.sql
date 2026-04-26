/*
  Mart Model: mart_stock_movements
  ===================================
  Aggregates stock movement activity by type and product category.
  Powers movement analysis in Power BI.

  Author: Lawrence Koomson
*/

with movements as (
    select * from "warehouse_db"."warehouse_dw"."stock_movements"
),

products as (
    select product_id, category, product_name
    from "warehouse_db"."warehouse_dw"."products"
),

joined as (

    select
        m.movement_id,
        m.product_id,
        p.product_name,
        p.category,
        m.movement_type,
        m.quantity,
        m.unit_cost_ghs,
        m.total_value_ghs,
        m.reference_no,
        m.movement_date,
        date_trunc('month', m.movement_date)            as movement_month

    from movements m
    join products p on m.product_id = p.product_id

),

aggregated as (

    select
        category,
        movement_type,
        date_trunc('month', movement_date)::date        as movement_month,

        count(movement_id)                              as total_movements,
        sum(quantity)                                   as total_quantity,
        round(sum(total_value_ghs), 2)                  as total_value_ghs,
        round(avg(total_value_ghs), 2)                  as avg_movement_value_ghs,
        round(avg(quantity), 1)                         as avg_quantity_per_movement

    from joined
    group by category, movement_type, date_trunc('month', movement_date)::date

)

select * from aggregated
order by movement_month, total_value_ghs desc