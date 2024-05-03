INSERT
    INTO
        ${catalog}.${database}.web_sales SELECT
            *
        FROM
            ${external_catalog}_${external_database}_web_sales;
