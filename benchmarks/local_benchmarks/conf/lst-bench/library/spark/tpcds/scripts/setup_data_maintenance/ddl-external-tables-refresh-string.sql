CREATE
    TEMP VIEW
        ${external_catalog}_${external_database}_s_catalog_page_${stream_num}(
            cpag_catalog_number INTEGER,
            cpag_catalog_page_number INTEGER,
            cpag_department STRING,
            cpag_id STRING,
            cpag_start_date STRING,
            cpag_end_date STRING,
            cpag_description STRING,
            cpag_type STRING
        )
            USING csv OPTIONS(
            PATH = "${external_data_path}${stream_num}/s_catalog_page_${stream_num}",
            sep = "|",
            header = "false",
            emptyValue = "",
            charset = "iso-8859-1",
            dateFormat = "yyyy-MM-dd",
            timestampFormat = "yyyy-MM-dd HH:mm:ss[.SSS]",
            mode = "PERMISSIVE",
            multiLine = "false",
            locale = "en-US",
            lineSep = "\n"
        );

CREATE
    TEMP VIEW
        ${external_catalog}_${external_database}_s_zip_to_gmt_${stream_num}(
            zipg_zip STRING,
            zipg_gmt_offset INTEGER
        )
            USING csv OPTIONS(
            PATH = "${external_data_path}${stream_num}/s_zip_to_gmt_${stream_num}",
            sep = "|",
            header = "false",
            emptyValue = "",
            charset = "iso-8859-1",
            dateFormat = "yyyy-MM-dd",
            timestampFormat = "yyyy-MM-dd HH:mm:ss[.SSS]",
            mode = "PERMISSIVE",
            multiLine = "false",
            locale = "en-US",
            lineSep = "\n"
        );

CREATE
    TEMP VIEW
        ${external_catalog}_${external_database}_s_purchase_lineitem_${stream_num}(
            plin_purchase_id INTEGER,
            plin_line_number INTEGER,
            plin_item_id STRING,
            plin_promotion_id STRING,
            plin_quantity BIGINT,
            plin_sale_price DECIMAL(
                7,
                2
            ),
            plin_coupon_amt DECIMAL(
                7,
                2
            ),
            plin_comment STRING
        )
            USING csv OPTIONS(
            PATH = "${external_data_path}${stream_num}/s_purchase_lineitem_${stream_num}",
            sep = "|",
            header = "false",
            emptyValue = "",
            charset = "iso-8859-1",
            dateFormat = "yyyy-MM-dd",
            timestampFormat = "yyyy-MM-dd HH:mm:ss[.SSS]",
            mode = "PERMISSIVE",
            multiLine = "false",
            locale = "en-US",
            lineSep = "\n"
        );

CREATE
    TEMP VIEW
        ${external_catalog}_${external_database}_s_customer_${stream_num}(
            cust_customer_id STRING,
            cust_salutation STRING,
            cust_last_name STRING,
            cust_first_name STRING,
            cust_preffered_flag STRING,
            cust_birth_date STRING,
            cust_birth_country STRING,
            cust_login_id STRING,
            cust_email_address STRING,
            cust_last_login_chg_date STRING,
            cust_first_shipto_date STRING,
            cust_first_purchase_date STRING,
            cust_last_review_date STRING,
            cust_primary_machine_id STRING,
            cust_secondary_machine_id STRING,
            cust_street_number SMALLINT,
            cust_suite_number STRING,
            cust_street_name1 STRING,
            cust_street_name2 STRING,
            cust_street_type STRING,
            cust_city STRING,
            cust_zip STRING,
            cust_county STRING,
            cust_state STRING,
            cust_country STRING,
            cust_loc_type STRING,
            cust_gender STRING,
            cust_marital_status STRING,
            cust_educ_status STRING,
            cust_credit_rating STRING,
            cust_purch_est DECIMAL(
                7,
                2
            ),
            cust_buy_potential STRING,
            cust_depend_cnt SMALLINT,
            cust_depend_emp_cnt SMALLINT,
            cust_depend_college_cnt SMALLINT,
            cust_vehicle_cnt SMALLINT,
            cust_annual_income DECIMAL(
                9,
                2
            )
        )
            USING csv OPTIONS(
            PATH = "${external_data_path}${stream_num}/s_customer_${stream_num}",
            sep = "|",
            header = "false",
            emptyValue = "",
            charset = "iso-8859-1",
            dateFormat = "yyyy-MM-dd",
            timestampFormat = "yyyy-MM-dd HH:mm:ss[.SSS]",
            mode = "PERMISSIVE",
            multiLine = "false",
            locale = "en-US",
            lineSep = "\n"
        );

CREATE
    TEMP VIEW
        ${external_catalog}_${external_database}_s_customer_address_${stream_num}(
            cadr_address_id STRING,
            cadr_street_number INTEGER,
            cadr_street_name1 STRING,
            cadr_street_name2 STRING,
            cadr_street_type STRING,
            cadr_suitdecimal STRING,
            cadr_city STRING,
            cadr_county STRING,
            cadr_state STRING,
            cadr_zip STRING,
            cadr_country STRING
        )
            USING csv OPTIONS(
            PATH = "${external_data_path}${stream_num}/s_customer_address_${stream_num}",
            sep = "|",
            header = "false",
            emptyValue = "",
            charset = "iso-8859-1",
            dateFormat = "yyyy-MM-dd",
            timestampFormat = "yyyy-MM-dd HH:mm:ss[.SSS]",
            mode = "PERMISSIVE",
            multiLine = "false",
            locale = "en-US",
            lineSep = "\n"
        );

CREATE
    TEMP VIEW
        ${external_catalog}_${external_database}_s_purchase_${stream_num}(
            purc_purchase_id INTEGER,
            purc_store_id STRING,
            purc_customer_id STRING,
            purc_purchase_date STRING,
            purc_purchase_time INTEGER,
            purc_register_id INTEGER,
            purc_clerk_id INTEGER,
            purc_comment STRING
        )
            USING csv OPTIONS(
            PATH = "${external_data_path}${stream_num}/s_purchase_${stream_num}",
            sep = "|",
            header = "false",
            emptyValue = "",
            charset = "iso-8859-1",
            dateFormat = "yyyy-MM-dd",
            timestampFormat = "yyyy-MM-dd HH:mm:ss[.SSS]",
            mode = "PERMISSIVE",
            multiLine = "false",
            locale = "en-US",
            lineSep = "\n"
        );


CREATE
    TEMP VIEW
        ${external_catalog}_${external_database}_s_catalog_order_${stream_num}(
            cord_order_id INTEGER,
            cord_bill_customer_id STRING,
            cord_ship_customer_id STRING,
            cord_order_date STRING,
            cord_order_time INTEGER,
            cord_ship_mode_id STRING,
            cord_call_center_id STRING,
            cord_order_comments STRING
        )
            USING csv OPTIONS(
            PATH = "${external_data_path}${stream_num}/s_catalog_order_${stream_num}",
            sep = "|",
            header = "false",
            emptyValue = "",
            charset = "iso-8859-1",
            dateFormat = "yyyy-MM-dd",
            timestampFormat = "yyyy-MM-dd HH:mm:ss[.SSS]",
            mode = "PERMISSIVE",
            multiLine = "false",
            locale = "en-US",
            lineSep = "\n"
        );


CREATE
    TEMP VIEW
        ${external_catalog}_${external_database}_s_web_order_${stream_num}(
            word_order_id INTEGER,
            word_bill_customer_id STRING,
            word_ship_customer_id STRING,
            word_order_date STRING,
            word_order_time INTEGER,
            word_ship_mode_id STRING,
            word_web_site_id STRING,
            word_order_comments STRING
        )
            USING csv OPTIONS(
            PATH = "${external_data_path}${stream_num}/s_web_order_${stream_num}",
            sep = "|",
            header = "false",
            emptyValue = "",
            charset = "iso-8859-1",
            dateFormat = "yyyy-MM-dd",
            timestampFormat = "yyyy-MM-dd HH:mm:ss[.SSS]",
            mode = "PERMISSIVE",
            multiLine = "false",
            locale = "en-US",
            lineSep = "\n"
        );


CREATE
    TEMP VIEW
        ${external_catalog}_${external_database}_s_item_${stream_num}(
            item_item_id STRING,
            item_item_description STRING,
            item_list_price DECIMAL(
                7,
                2
            ),
            item_wholesale_cost DECIMAL(
                7,
                2
            ),
            item_size STRING,
            item_formulation STRING,
            item_color STRING,
            item_units STRING,
            item_container STRING,
            item_manager_id INTEGER
        )
            USING csv OPTIONS(
            PATH = "${external_data_path}${stream_num}/s_item_${stream_num}",
            sep = "|",
            header = "false",
            emptyValue = "",
            charset = "iso-8859-1",
            dateFormat = "yyyy-MM-dd",
            timestampFormat = "yyyy-MM-dd HH:mm:ss[.SSS]",
            mode = "PERMISSIVE",
            multiLine = "false",
            locale = "en-US",
            lineSep = "\n"
        );


CREATE
    TEMP VIEW
        ${external_catalog}_${external_database}_s_catalog_order_lineitem_${stream_num}(
            clin_order_id INTEGER,
            clin_line_number INTEGER,
            clin_item_id STRING,
            clin_promotion_id STRING,
            clin_quantity BIGINT,
            clin_sales_price DECIMAL(
                7,
                2
            ),
            clin_coupon_amt DECIMAL(
                7,
                2
            ),
            clin_warehouse_id STRING,
            clin_ship_date STRING,
            clin_catalog_number INTEGER,
            clin_catalog_page_number INTEGER,
            clin_ship_cost DECIMAL(
                7,
                2
            )
        )
            USING csv OPTIONS(
            PATH = "${external_data_path}${stream_num}/s_catalog_order_lineitem_${stream_num}",
            sep = "|",
            header = "false",
            emptyValue = "",
            charset = "iso-8859-1",
            dateFormat = "yyyy-MM-dd",
            timestampFormat = "yyyy-MM-dd HH:mm:ss[.SSS]",
            mode = "PERMISSIVE",
            multiLine = "false",
            locale = "en-US",
            lineSep = "\n"
        );


CREATE
    TEMP VIEW
        ${external_catalog}_${external_database}_s_web_order_lineitem_${stream_num}(
            wlin_order_id INTEGER,
            wlin_line_number INTEGER,
            wlin_item_id STRING,
            wlin_promotion_id STRING,
            wlin_quantity BIGINT,
            wlin_sales_price DECIMAL(
                7,
                2
            ),
            wlin_coupon_amt DECIMAL(
                7,
                2
            ),
            wlin_warehouse_id STRING,
            wlin_ship_date STRING,
            wlin_ship_cost DECIMAL(
                7,
                2
            ),
            wlin_web_page_id STRING
        )
            USING csv OPTIONS(
            PATH = "${external_data_path}${stream_num}/s_web_order_lineitem_${stream_num}",
            sep = "|",
            header = "false",
            emptyValue = "",
            charset = "iso-8859-1",
            dateFormat = "yyyy-MM-dd",
            timestampFormat = "yyyy-MM-dd HH:mm:ss[.SSS]",
            mode = "PERMISSIVE",
            multiLine = "false",
            locale = "en-US",
            lineSep = "\n"
        );


CREATE
    TEMP VIEW
        ${external_catalog}_${external_database}_s_store_${stream_num}(
            stor_store_id STRING,
            stor_closed_date STRING,
            stor_name STRING,
            stor_employees INTEGER,
            stor_floor_space INTEGER,
            stor_hours STRING,
            stor_store_manager STRING,
            stor_market_id INTEGER,
            stor_geography_class STRING,
            stor_market_manager STRING,
            stor_tax_percentage DECIMAL(
                5,
                2
            )
        )
            USING csv OPTIONS(
            PATH = "${external_data_path}${stream_num}/s_store_${stream_num}",
            sep = "|",
            header = "false",
            emptyValue = "",
            charset = "iso-8859-1",
            dateFormat = "yyyy-MM-dd",
            timestampFormat = "yyyy-MM-dd HH:mm:ss[.SSS]",
            mode = "PERMISSIVE",
            multiLine = "false",
            locale = "en-US",
            lineSep = "\n"
        );


CREATE
    TEMP VIEW
        ${external_catalog}_${external_database}_s_call_center_${stream_num}(
            call_center_id STRING,
            call_open_date STRING,
            call_closed_date STRING,
            call_center_name STRING,
            call_center_class STRING,
            call_center_employees INTEGER,
            call_center_sq_ft INTEGER,
            call_center_hours STRING,
            call_center_manager STRING,
            call_center_tax_percentage DECIMAL(
                7,
                2
            )
        )
            USING csv OPTIONS(
            PATH = "${external_data_path}${stream_num}/s_call_center_${stream_num}",
            sep = "|",
            header = "false",
            emptyValue = "",
            charset = "iso-8859-1",
            dateFormat = "yyyy-MM-dd",
            timestampFormat = "yyyy-MM-dd HH:mm:ss[.SSS]",
            mode = "PERMISSIVE",
            multiLine = "false",
            locale = "en-US",
            lineSep = "\n"
        );


CREATE
    TEMP VIEW
        ${external_catalog}_${external_database}_s_web_site_${stream_num}(
            wsit_web_site_id STRING,
            wsit_open_date STRING,
            wsit_closed_date STRING,
            wsit_site_name STRING,
            wsit_site_class STRING,
            wsit_site_manager STRING,
            wsit_tax_percentage DECIMAL(
                            5,
                            2
                        )
                    )
                        USING csv OPTIONS(
                        PATH = "${external_data_path}${stream_num}/s_web_site_${stream_num}",
                        sep = "|",
                        header = "false",
                        emptyValue = "",
                        charset = "iso-8859-1",
                        dateFormat = "yyyy-MM-dd",
                        timestampFormat = "yyyy-MM-dd HH:mm:ss[.SSS]",
                        mode = "PERMISSIVE",
                        multiLine = "false",
                        locale = "en-US",
                        lineSep = "\n"
                    );


CREATE
    TEMP VIEW
        ${external_catalog}_${external_database}_s_warehouse_${stream_num}(
            wrhs_warehouse_id STRING,
            wrhs_warehouse_desc STRING,
            wrhs_warehouse_sq_ft INTEGER
        )
            USING csv OPTIONS(
            PATH = "${external_data_path}${stream_num}/s_warehouse_${stream_num}",
            sep = "|",
            header = "false",
            emptyValue = "",
            charset = "iso-8859-1",
            dateFormat = "yyyy-MM-dd",
            timestampFormat = "yyyy-MM-dd HH:mm:ss[.SSS]",
            mode = "PERMISSIVE",
            multiLine = "false",
            locale = "en-US",
            lineSep = "\n"
        );


CREATE
    TEMP VIEW
        ${external_catalog}_${external_database}_s_web_page_${stream_num}(
            wpag_web_page_id STRING,
            wpag_create_date STRING,
            wpag_access_date STRING,
            wpag_autogen_flag STRING,
            wpag_url STRING,
            wpag_type STRING,
            wpag_char_cnt INTEGER,
            wpag_link_cnt INTEGER,
            wpag_image_cnt INTEGER,
            wpag_max_ad_cnt INTEGER
        )
            USING csv OPTIONS(
            PATH = "${external_data_path}${stream_num}/s_web_page_${stream_num}",
            sep = "|",
            header = "false",
            emptyValue = "",
            charset = "iso-8859-1",
            dateFormat = "yyyy-MM-dd",
            timestampFormat = "yyyy-MM-dd HH:mm:ss[.SSS]",
            mode = "PERMISSIVE",
            multiLine = "false",
            locale = "en-US",
            lineSep = "\n"
        );


CREATE
    TEMP VIEW
        ${external_catalog}_${external_database}_s_promotion_${stream_num}(
            prom_promotion_id STRING,
            prom_promotion_name STRING,
            prom_start_date STRING,
            prom_end_date STRING,
            prom_cost DECIMAL(
                7,
                2
            ),
            prom_response_target STRING,
            prom_channel_dmail STRING,
            prom_channel_email STRING,
            prom_channel_catalog STRING,
            prom_channel_tv STRING,
            prom_channel_radio STRING,
            prom_channel_press STRING,
            prom_channel_event STRING,
            prom_channel_demo STRING,
            prom_channel_details STRING,
            prom_purpose STRING,
            prom_discount_active STRING,
            prom_discount_pct DECIMAL(
                5,
                2
            )
        )
            USING csv OPTIONS(
            PATH = "${external_data_path}${stream_num}/s_promotion_${stream_num}",
            sep = "|",
            header = "false",
            emptyValue = "",
            charset = "iso-8859-1",
            dateFormat = "yyyy-MM-dd",
            timestampFormat = "yyyy-MM-dd HH:mm:ss[.SSS]",
            mode = "PERMISSIVE",
            multiLine = "false",
            locale = "en-US",
            lineSep = "\n"
        );


CREATE
    TEMP VIEW
        ${external_catalog}_${external_database}_s_store_returns_${stream_num}(
            sret_store_id STRING,
            sret_purchase_id STRING,
            sret_line_number INTEGER,
            sret_item_id STRING,
            sret_customer_id STRING,
            sret_return_date STRING,
            sret_return_time STRING,
            sret_ticket_number STRING,
            sret_return_qty INTEGER,
            sret_return_amt DECIMAL(
                7,
                2
            ),
            sret_return_tax DECIMAL(
                7,
                2
            ),
            sret_return_fee DECIMAL(
                7,
                2
            ),
            sret_return_ship_cost DECIMAL(
                7,
                2
            ),
            sret_refunded_cash DECIMAL(
                7,
                2
            ),
            sret_reversed_charge DECIMAL(
                7,
                2
            ),
            sret_store_credit DECIMAL(
                7,
                2
            ),
            sret_reason_id STRING
        )
            USING csv OPTIONS(
            PATH = "${external_data_path}${stream_num}/s_store_returns_${stream_num}",
            sep = "|",
            header = "false",
            emptyValue = "",
            charset = "iso-8859-1",
            dateFormat = "yyyy-MM-dd",
            timestampFormat = "yyyy-MM-dd HH:mm:ss[.SSS]",
            mode = "PERMISSIVE",
            multiLine = "false",
            locale = "en-US",
            lineSep = "\n"
        );


CREATE
    TEMP VIEW
        ${external_catalog}_${external_database}_s_catalog_returns_${stream_num}(
            cret_call_center_id STRING,
            cret_order_id INTEGER,
            cret_line_number INTEGER,
            cret_item_id STRING,
            cret_return_customer_id STRING,
            cret_refund_customer_id STRING,
            cret_return_date STRING,
            cret_return_time STRING,
            cret_return_qty INTEGER,
            cret_return_amt DECIMAL(
                7,
                2
            ),
            cret_return_tax DECIMAL(
                7,
                2
            ),
            cret_return_fee DECIMAL(
                7,
                2
            ),
            cret_return_ship_cost DECIMAL(
                7,
                2
            ),
            cret_refunded_cash DECIMAL(
                7,
                2
            ),
            cret_reversed_charge DECIMAL(
                7,
                2
            ),
            cret_merchant_credit DECIMAL(
                7,
                2
            ),
            cret_reason_id STRING,
            cret_shipmode_id STRING,
            cret_catalog_page_id STRING,
            cret_warehouse_id STRING
        )
            USING csv OPTIONS(
            PATH = "${external_data_path}${stream_num}/s_catalog_returns_${stream_num}",
            sep = "|",
            header = "false",
            emptyValue = "",
            charset = "iso-8859-1",
            dateFormat = "yyyy-MM-dd",
            timestampFormat = "yyyy-MM-dd HH:mm:ss[.SSS]",
            mode = "PERMISSIVE",
            multiLine = "false",
            locale = "en-US",
            lineSep = "\n"
        );


CREATE
    TEMP VIEW
        ${external_catalog}_${external_database}_s_web_returns_${stream_num}(
            wret_web_site_id STRING,
            wret_order_id INTEGER,
            wret_line_number INTEGER,
            wret_item_id STRING,
            wret_return_customer_id STRING,
            wret_refund_customer_id STRING,
            wret_return_date STRING,
            wret_return_time STRING,
            wret_return_qty INTEGER,
            wret_return_amt DECIMAL(
                7,
                2
            ),
            wret_return_tax DECIMAL(
                7,
                2
            ),
            wret_return_fee DECIMAL(
                7,
                2
            ),
            wret_return_ship_cost DECIMAL(
                7,
                2
            ),
            wret_refunded_cash DECIMAL(
                7,
                2
            ),
            wret_reversed_charge DECIMAL(
                7,
                2
            ),
            wret_account_credit DECIMAL(
                7,
                2
            ),
            wret_reason_id STRING
        )
            USING csv OPTIONS(
            PATH = "${external_data_path}${stream_num}/s_web_returns_${stream_num}",
            sep = "|",
            header = "false",
            emptyValue = "",
            charset = "iso-8859-1",
            dateFormat = "yyyy-MM-dd",
            timestampFormat = "yyyy-MM-dd HH:mm:ss[.SSS]",
            mode = "PERMISSIVE",
            multiLine = "false",
            locale = "en-US",
            lineSep = "\n"
        );


CREATE
    TEMP VIEW
        ${external_catalog}_${external_database}_s_inventory_${stream_num}(
            invn_warehouse_id STRING,
            invn_item_id STRING,
            invn_date STRING,
            invn_qty_on_hand INTEGER
        )
            USING csv OPTIONS(
            PATH = "${external_data_path}${stream_num}/s_inventory_${stream_num}",
            sep = "|",
            header = "false",
            emptyValue = "",
            charset = "iso-8859-1",
            dateFormat = "yyyy-MM-dd",
            timestampFormat = "yyyy-MM-dd HH:mm:ss[.SSS]",
            mode = "PERMISSIVE",
            multiLine = "false",
            locale = "en-US",
            lineSep = "\n"
        );
