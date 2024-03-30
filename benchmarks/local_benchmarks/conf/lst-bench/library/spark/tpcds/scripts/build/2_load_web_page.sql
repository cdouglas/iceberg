INSERT
    INTO
        ${catalog}.${database}.web_page SELECT
            *
        FROM
            ${external_catalog}_${external_database}_web_page;
