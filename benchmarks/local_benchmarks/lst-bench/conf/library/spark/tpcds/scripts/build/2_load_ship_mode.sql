INSERT
    INTO
        ${catalog}.${database}.ship_mode SELECT
            *
        FROM
            ${external_catalog}_${external_database}_ship_mode;
