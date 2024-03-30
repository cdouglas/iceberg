INSERT
    INTO
        ${catalog}.${database}.reason SELECT
            *
        FROM
            ${external_catalog}_${external_database}_reason;
