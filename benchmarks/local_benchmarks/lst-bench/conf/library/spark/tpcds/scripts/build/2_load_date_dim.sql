INSERT
    INTO
        ${catalog}.${database}.date_dim SELECT
            *
        FROM
            ${external_catalog}_${external_database}_date_dim;
