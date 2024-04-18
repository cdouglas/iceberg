INSERT INTO ${catalog}.${database}.store_sales(
    ss_sold_time_sk,
    ss_item_sk,
    ss_customer_sk,
    ss_cdemo_sk,
    ss_hdemo_sk,
    ss_addr_sk,
    ss_store_sk,
    ss_promo_sk,
    ss_ticket_number,
    ss_quantity,
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
    ss_sold_date_sk
) VALUES (
    1,  -- ss_sold_time_sk
    2,  -- ss_item_sk
    3,  -- ss_customer_sk
    4,  -- ss_cdemo_sk
    5,  -- ss_hdemo_sk
    6,  -- ss_addr_sk
    7,  -- ss_store_sk
    8,  -- ss_promo_sk
    9,  -- ss_ticket_number
    10, -- ss_quantity
    11.50, -- ss_wholesale_cost
    15.99, -- ss_list_price
    14.99, -- ss_sales_price
    1.00, -- ss_ext_discount_amt
    13.99, -- ss_ext_sales_price
    11.50, -- ss_ext_wholesale_cost
    15.99, -- ss_ext_list_price
    1.08, -- ss_ext_tax
    0.50, -- ss_coupon_amt
    14.49, -- ss_net_paid
    15.57, -- ss_net_paid_inc_tax
    3.49, -- ss_net_profit
    20230101  -- ss_sold_date_sk (partition key!)
);