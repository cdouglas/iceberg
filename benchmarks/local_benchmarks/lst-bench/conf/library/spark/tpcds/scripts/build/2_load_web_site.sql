INSERT
    INTO
        ${catalog}.${database}.web_site SELECT
            *
        FROM
            ${external_catalog}_${external_database}_web_site;
