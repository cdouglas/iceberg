/* This query scans the leading part of the source store_sales tables until ss_item_sk < X.  The amount of work is independent of the source tables. */
/* Then it makes a non-conflicting modification, an update when the predicate in line 36 evaluates true otherwise an insert */
MERGE INTO ${catalog}.${database}.${target} target
    USING (
        SELECT
        99999999 as ss_item_sk,
        1 as ss_ticket_number,
        ss_sold_time_sk,
        ss_customer_sk,
        ss_cdemo_sk,
        ss_hdemo_sk,
        ss_addr_sk,
        ss_store_sk,
        ss_promo_sk,
        ss_wholesale_cost,
        ss_list_price,
        ss_sales_price,
        ss_ext_discount_amt,
        ss_ext_sales_price,
        ss_ext_wholesale_cost,
        ss_ext_list_price,
        ss_ext_tax,
        ss_coupon_amt,
        ss_net_paid,
        ss_net_paid_inc_tax,
        ss_net_profit,
        ss_sold_date_sk,
        (c1 + c2) as ss_quantity
        FROM
            (SELECT count(*) as c1 FROM ${catalog}.${database}.${source_1} WHERE ss_item_sk < ${item_max}),
            (SELECT count(*) as c2 FROM ${catalog}.${database}.${source_2} WHERE ss_item_sk < ${item_max}+1),
            (SELECT * from ${catalog}.${database}.${source_1} WHERE ss_item_sk = 28800 and ss_ticket_number = 78)
    ) source
    ON
    target.ss_item_sk = 75599 and target.ss_ticket_number = 498
    and (1 = ${do_update})
    WHEN MATCHED THEN UPDATE SET target.ss_quantity = source.ss_quantity
    WHEN NOT MATCHED THEN INSERT *;