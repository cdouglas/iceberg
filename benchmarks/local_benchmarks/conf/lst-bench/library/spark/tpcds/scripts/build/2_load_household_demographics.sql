INSERT
    INTO
        ${catalog}.${database}.household_demographics SELECT
            *
        FROM
            ${external_catalog}_${external_database}_household_demographics;
