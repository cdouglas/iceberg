INSERT
    INTO
        ${catalog}.${database}.catalog_returns SELECT
            *
        FROM
            ${external_catalog}_${external_database}_catalog_returns;
