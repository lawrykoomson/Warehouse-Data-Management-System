/*
  Mart Model: mart_supplier_performance
  =======================================
  Aggregates supplier KPIs including order fulfillment
  and inventory contribution.
  Powers supplier analysis in Power BI.

  Author: Lawrence Koomson
*/

with suppliers as (
    select * from {{ source('warehouse_dw', 'suppliers') }}
),

orders as (
    select * from {{ source('warehouse_dw', 'purchase_orders') }}
),

inventory as (
    select * from {{ ref('stg_inventory') }}
),

supplier_orders as (

    select
        s.supplier_id,
        s.supplier_name,
        s.region,
        s.category,
        s.rating,
        s.is_active,

        count(o.po_id)                                  as total_orders,
        count(case when o.status = 'RECEIVED'
                   then 1 end)                          as received_orders,
        count(case when o.status = 'PENDING'
                   then 1 end)                          as pending_orders,
        count(case when o.status = 'CANCELLED'
                   then 1 end)                          as cancelled_orders,

        round(
            count(case when o.status = 'RECEIVED' then 1 end)::numeric
            / nullif(count(o.po_id), 0) * 100
        , 2)                                            as fulfillment_rate_pct,

        round(sum(o.total_cost_ghs), 2)                 as total_order_value_ghs,
        round(avg(o.total_cost_ghs), 2)                 as avg_order_value_ghs,
        round(avg(o.quantity_ordered), 1)               as avg_order_quantity

    from suppliers s
    left join orders o on s.supplier_id = o.supplier_id
    group by
        s.supplier_id, s.supplier_name, s.region,
        s.category, s.rating, s.is_active

),

with_inventory as (

    select
        so.*,
        count(inv.product_id)                           as products_supplied,
        round(sum(inv.stock_value_ghs), 2)              as inventory_value_ghs

    from supplier_orders so
    left join inventory inv on so.supplier_id = inv.supplier_id
    group by
        so.supplier_id, so.supplier_name, so.region,
        so.category, so.rating, so.is_active,
        so.total_orders, so.received_orders, so.pending_orders,
        so.cancelled_orders, so.fulfillment_rate_pct,
        so.total_order_value_ghs, so.avg_order_value_ghs,
        so.avg_order_quantity

)

select * from with_inventory
order by fulfillment_rate_pct desc, total_order_value_ghs desc