INSERT
    INTO
        ${catalog}.${database}.time_dim SELECT
            *
        FROM
            ${external_catalog}_${external_database}_time_dim;
