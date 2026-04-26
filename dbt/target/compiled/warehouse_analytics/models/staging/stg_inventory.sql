/*
  Staging Model: stg_inventory
  ==============================
  Joins inventory with products and suppliers to create
  a complete view of current warehouse stock status.

  Source: warehouse_dw (5 tables)
  Author: Lawrence Koomson
*/

with products as (
    select * from "warehouse_db"."warehouse_dw"."products"
),

suppliers as (
    select * from "warehouse_db"."warehouse_dw"."suppliers"
),

inventory as (
    select * from "warehouse_db"."warehouse_dw"."inventory"
),

joined as (

    select
        i.inventory_id,
        i.product_id,
        p.product_name,
        p.category,
        p.unit_of_measure,
        p.unit_cost_ghs,
        p.unit_price_ghs,
        p.reorder_point,
        p.reorder_qty,

        s.supplier_id,
        s.supplier_name,
        s.region                                        as supplier_region,
        s.rating                                        as supplier_rating,

        i.quantity_on_hand,
        i.quantity_reserved,
        i.quantity_available,
        i.warehouse_location,
        i.last_counted_date,
        i.needs_reorder,
        i.updated_at,

        round(i.quantity_on_hand * p.unit_cost_ghs, 2) as stock_value_ghs,
        round(i.quantity_on_hand * p.unit_price_ghs, 2) as stock_retail_value_ghs,

        case
            when i.quantity_on_hand = 0            then 'Out of Stock'
            when i.needs_reorder                   then 'Low Stock'
            when i.quantity_on_hand < p.reorder_point * 2 then 'Adequate'
            else 'Well Stocked'
        end                                             as stock_status,

        case
            when p.unit_price_ghs > 0
            then round((p.unit_price_ghs - p.unit_cost_ghs) / p.unit_price_ghs * 100, 2)
            else 0
        end                                             as margin_pct

    from inventory i
    join products p  on i.product_id  = p.product_id
    join suppliers s on p.supplier_id = s.supplier_id
    where p.is_active = true

)

select * from joined