/*
  Mart Model: mart_inventory_status
  ====================================
  Aggregates inventory KPIs by category and stock status.
  Powers inventory dashboard in Power BI.

  Author: Lawrence Koomson
*/

with staged as (

    select * from "warehouse_db"."warehouse_dw_staging"."stg_inventory"

),

inventory_status as (

    select
        category,
        stock_status,

        count(product_id)                               as product_count,
        sum(quantity_on_hand)                           as total_units,
        sum(quantity_available)                         as available_units,
        sum(quantity_reserved)                          as reserved_units,

        round(sum(stock_value_ghs), 2)                  as total_stock_value_ghs,
        round(sum(stock_retail_value_ghs), 2)           as total_retail_value_ghs,
        round(avg(margin_pct), 2)                       as avg_margin_pct,
        round(avg(unit_cost_ghs), 2)                    as avg_unit_cost_ghs,

        count(case when needs_reorder then 1 end)       as reorder_needed_count,

        round(
            sum(stock_value_ghs)
            / sum(sum(stock_value_ghs)) over () * 100
        , 2)                                            as value_share_pct

    from staged
    group by category, stock_status

)

select * from inventory_status
order by total_stock_value_ghs desc