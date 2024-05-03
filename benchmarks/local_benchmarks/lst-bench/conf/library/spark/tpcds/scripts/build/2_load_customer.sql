INSERT
    INTO
        ${catalog}.${database}.customer SELECT
            *
        FROM
            ${external_catalog}_${external_database}_customer;
