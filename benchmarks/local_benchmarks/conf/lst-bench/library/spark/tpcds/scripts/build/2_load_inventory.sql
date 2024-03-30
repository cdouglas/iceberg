INSERT
    INTO
        ${catalog}.${database}.inventory SELECT
            *
        FROM
            ${external_catalog}_${external_database}_inventory;
