INSERT
    INTO
        ${catalog}.${database}.customer_demographics SELECT
            *
        FROM
            ${external_catalog}_${external_database}_customer_demographics;
