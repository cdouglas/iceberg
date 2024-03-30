INSERT
    INTO
        ${catalog}.${database}.customer_address SELECT
            *
        FROM
            ${external_catalog}_${external_database}_customer_address;
