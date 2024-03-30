WITH customer_total_return AS(
    SELECT
        sr_customer_sk AS ctr_customer_sk,
        sr_store_sk AS ctr_store_sk,
        SUM( SR_RETURN_AMT_INC_TAX ) AS ctr_total_return
    FROM
        ${catalog}.${database}.store_returns ${asof},
        ${catalog}.${database}.date_dim
    WHERE
        sr_returned_date_sk = d_date_sk
        AND d_year = 1999
    GROUP BY
        sr_customer_sk,
        sr_store_sk
) SELECT
    c_customer_id
FROM
    customer_total_return ctr1,
    ${catalog}.${database}.store,
    ${catalog}.${database}.customer
WHERE
    ctr1.ctr_total_return >(
        SELECT
            AVG( ctr_total_return )* 1.2
        FROM
            customer_total_return ctr2
        WHERE
            ctr1.ctr_store_sk = ctr2.ctr_store_sk
    )
    AND s_store_sk = ctr1.ctr_store_sk
    AND s_state = 'TN'
    AND ctr1.ctr_customer_sk = c_customer_sk
ORDER BY
    c_customer_id LIMIT 100;
