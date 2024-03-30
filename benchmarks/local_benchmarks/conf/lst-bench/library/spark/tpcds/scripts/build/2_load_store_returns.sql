INSERT
    INTO
        ${catalog}.${database}.store_returns SELECT
            *
        FROM
            ${external_catalog}_${external_database}_store_returns;
