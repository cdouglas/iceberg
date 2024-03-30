INSERT
    INTO
        ${catalog}.${database}.web_returns SELECT
            *
        FROM
            ${external_catalog}_${external_database}_web_returns;
