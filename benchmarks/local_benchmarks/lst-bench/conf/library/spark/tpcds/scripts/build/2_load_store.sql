INSERT
    INTO
        ${catalog}.${database}.store SELECT
            *
        FROM
            ${external_catalog}_${external_database}_store;
