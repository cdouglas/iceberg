INSERT
    INTO
        ${catalog}.${database}.catalog_sales SELECT
            *
        FROM
            ${external_catalog}_${external_database}_catalog_sales;
