INSERT
    INTO
        ${catalog}.${database}.warehouse SELECT
            *
        FROM
            ${external_catalog}_${external_database}_warehouse;
