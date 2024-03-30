INSERT
    INTO
        ${catalog}.${database}.store_sales SELECT
            *
        FROM
            ${external_catalog}_${external_database}_store_sales;
