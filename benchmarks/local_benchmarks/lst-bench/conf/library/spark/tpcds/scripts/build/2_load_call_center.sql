INSERT
    INTO
        ${catalog}.${database}.call_center SELECT
            *
        FROM
            ${external_catalog}_${external_database}_call_center;
