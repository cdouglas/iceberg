INSERT
    INTO
        ${catalog}.${database}.item SELECT
            *
        FROM
            ${external_catalog}_${external_database}_item;
