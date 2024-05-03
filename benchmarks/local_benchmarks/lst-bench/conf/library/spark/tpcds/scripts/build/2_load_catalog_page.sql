INSERT
    INTO
        ${catalog}.${database}.catalog_page SELECT
            *
        FROM
            ${external_catalog}_${external_database}_catalog_page;
