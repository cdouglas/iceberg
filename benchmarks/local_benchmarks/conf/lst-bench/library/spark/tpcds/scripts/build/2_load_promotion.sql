INSERT
    INTO
        ${catalog}.${database}.promotion SELECT
            *
        FROM
            ${external_catalog}_${external_database}_promotion;
