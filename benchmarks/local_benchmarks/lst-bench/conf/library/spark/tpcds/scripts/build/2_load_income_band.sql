INSERT
    INTO
        ${catalog}.${database}.income_band SELECT
            *
        FROM
            ${external_catalog}_${external_database}_income_band;
