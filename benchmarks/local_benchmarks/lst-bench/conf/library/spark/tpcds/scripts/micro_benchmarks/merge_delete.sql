MERGE INTO
            ${catalog}.${database}.store_sales
                USING(
                SELECT
                    *
                FROM
                    (
                        SELECT
                            MIN( d_date_sk ) AS min_date
                        FROM
                            ${catalog}.${database}.date_dim
                        WHERE
                            d_date BETWEEN '${param1}' AND '${param2}'
                    ) r
                JOIN(
                        SELECT
                            MAX( d_date_sk ) AS max_date
                        FROM
                            ${catalog}.${database}.date_dim
                        WHERE
                            d_date BETWEEN '${param1}' AND '${param2}'
                    ) s
            ) SOURCE ON
            ss_sold_date_sk >= min_date
            AND ss_sold_date_sk <= max_date
            WHEN MATCHED THEN DELETE;