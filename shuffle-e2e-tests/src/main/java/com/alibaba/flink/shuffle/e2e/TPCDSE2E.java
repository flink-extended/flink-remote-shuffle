/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.alibaba.flink.shuffle.e2e;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** TPCDS end to end test. */
public class TPCDSE2E {

    private static final Logger LOG = LoggerFactory.getLogger(TPCDSE2E.class);

    public static void main(String[] args) {
        if (args.length != 3) {
            throw new IllegalArgumentException("Args: (tpcds-home, parallelism, sqlIdx).");
        }
        String tpcdsHome = args[0];
        int parallelism = Integer.valueOf(args[1]);
        LOG.info("Running E2E under TPCDS: {}.", tpcdsHome);

        EnvironmentSettings settings = EnvironmentSettings.newInstance().inBatchMode().build();
        TableEnvironment stEnv = TableEnvironment.create(settings);
        Configuration configuration = stEnv.getConfig().getConfiguration();
        configuration.setInteger("table.exec.resource.default-parallelism", parallelism);
        configuration.setString("table.exec.shuffle-mode", "ALL_EDGES_BLOCKING");

        registerTables(stEnv, tpcdsHome);

        Integer sqlIdx = Integer.valueOf(args[2]);
        stEnv.executeSql(sqls()[sqlIdx]).collect();
    }

    private static String[] sqls() {
        String[] sqls = new String[3];
        sqls[0] =
                ""
                        + "insert into result_table select count(1) from (select distinct * from store_sales) x";
        sqls[1] =
                ""
                        + "insert into result_table"
                        + " select count(1) from"
                        + " (select * from store_sales union select * from store_sales union select * from store_sales union select * from store_sales) x";
        sqls[2] =
                ""
                        + "insert into result_table"
                        + " select count(1) from"
                        + " (select distinct * from"
                        + "  store_sales join item on store_sales.ss_item_sk=item.i_item_sk "
                        + "              join date_dim on store_sales.ss_sold_date_sk=d_date_sk) t";
        return sqls;
    }

    private static String createTableFooter(String tpcdsHome, String tablePath) {
        return String.format(
                ""
                        + "WITH (\n"
                        + "  'connector' = 'filesystem',\n"
                        + "  'path' = '%s/%s',\n"
                        + "  'format' = 'csv',\n"
                        + "  'csv.field-delimiter' = '|',\n"
                        + "  'csv.ignore-parse-errors' = 'true'\n"
                        + ")",
                tpcdsHome, tablePath);
    }

    private static void registerTables(TableEnvironment tenv, String tpcdsHome) {
        tenv.executeSql(
                ""
                        + "CREATE TABLE result_table (res BIGINT)\n"
                        + createTableFooter(tpcdsHome, "result/"));
        tenv.executeSql(
                ""
                        + "CREATE TABLE store_sales (\n"
                        + "  ss_sold_date_sk BIGINT,\n"
                        + "  ss_sold_time_sk BIGINT,\n"
                        + "  ss_item_sk BIGINT,\n"
                        + "  ss_customer_sk BIGINT,\n"
                        + "  ss_cdemo_sk BIGINT,\n"
                        + "  ss_hdemo_sk BIGINT,\n"
                        + "  ss_addr_sk BIGINT,\n"
                        + "  ss_store_sk BIGINT,\n"
                        + "  ss_promo_sk BIGINT,\n"
                        + "  ss_ticket_number BIGINT,\n"
                        + "  ss_quantity BIGINT,\n"
                        + "  ss_wholesale_cost DOUBLE,\n"
                        + "  ss_list_price DOUBLE,\n"
                        + "  ss_sales_price DOUBLE,\n"
                        + "  ss_ext_discount_amt DOUBLE,\n"
                        + "  ss_ext_sales_price DOUBLE,\n"
                        + "  ss_ext_wholesale_cost DOUBLE,\n"
                        + "  ss_ext_list_price DOUBLE,\n"
                        + "  ss_ext_tax DOUBLE,\n"
                        + "  ss_coupon_amt DOUBLE,\n"
                        + "  ss_net_paid DOUBLE,\n"
                        + "  ss_net_paid_inc_tax DOUBLE,\n"
                        + "  ss_net_profit DOUBLE)\n"
                        + createTableFooter(tpcdsHome, "store_sales/"));

        tenv.executeSql(
                ""
                        + "CREATE TABLE web_sales (\n"
                        + "  ws_sold_date_sk BIGINT,\n"
                        + "  ws_sold_time_sk BIGINT,\n"
                        + "  ws_ship_date_sk BIGINT,\n"
                        + "  ws_item_sk BIGINT,\n"
                        + "  ws_bill_customer_sk BIGINT,\n"
                        + "  ws_bill_cdemo_sk BIGINT,\n"
                        + "  ws_bill_hdemo_sk BIGINT,\n"
                        + "  ws_bill_addr_sk BIGINT,\n"
                        + "  ws_ship_customer_sk BIGINT,\n"
                        + "  ws_ship_cdemo_sk BIGINT,\n"
                        + "  ws_ship_hdemo_sk BIGINT,\n"
                        + "  ws_ship_addr_sk BIGINT,\n"
                        + "  ws_web_page_sk BIGINT,\n"
                        + "  ws_web_site_sk BIGINT,\n"
                        + "  ws_ship_mode_sk BIGINT,\n"
                        + "  ws_warehouse_sk BIGINT,\n"
                        + "  ws_promo_sk BIGINT,\n"
                        + "  ws_order_number BIGINT,\n"
                        + "  ws_quantity BIGINT,\n"
                        + "  ws_wholesale_cost DOUBLE,\n"
                        + "  ws_list_price DOUBLE,\n"
                        + "  ws_sales_price DOUBLE,\n"
                        + "  ws_ext_discount_amt DOUBLE,\n"
                        + "  ws_ext_sales_price DOUBLE,\n"
                        + "  ws_ext_wholesale_cost DOUBLE,\n"
                        + "  ws_ext_list_price DOUBLE,\n"
                        + "  ws_ext_tax DOUBLE,\n"
                        + "  ws_coupon_amt DOUBLE,\n"
                        + "  ws_ext_ship_cost DOUBLE,\n"
                        + "  ws_net_paid DOUBLE,\n"
                        + "  ws_net_paid_inc_tax DOUBLE,\n"
                        + "  ws_net_paid_inc_ship DOUBLE,\n"
                        + "  ws_net_paid_inc_ship_tax DOUBLE,\n"
                        + "  ws_net_profit DOUBLE)\n"
                        + createTableFooter(tpcdsHome, "web_sales/"));
        tenv.executeSql(
                ""
                        + "CREATE TABLE item (\n"
                        + "  i_item_sk BIGINT,\n"
                        + "  i_item_id STRING,\n"
                        + "  i_rec_start_date STRING,\n"
                        + "  i_rec_end_date STRING,\n"
                        + "  i_item_desc STRING,\n"
                        + "  i_current_price DOUBLE,\n"
                        + "  i_wholesale_cost DOUBLE,\n"
                        + "  i_brand_id BIGINT,\n"
                        + "  i_brand STRING,\n"
                        + "  i_class_id BIGINT,\n"
                        + "  i_class STRING,\n"
                        + "  i_category_id BIGINT,\n"
                        + "  i_category STRING,\n"
                        + "  i_manufact_id BIGINT,\n"
                        + "  i_manufact STRING,\n"
                        + "  i_size STRING,\n"
                        + "  i_formulation STRING,\n"
                        + "  i_color STRING,\n"
                        + "  i_units STRING,\n"
                        + "  i_container STRING,\n"
                        + "  i_manager_id BIGINT,\n"
                        + "  i_product_name STRING)\n"
                        + createTableFooter(tpcdsHome, "item/"));

        tenv.executeSql(
                ""
                        + "CREATE TABLE date_dim (\n"
                        + "  d_date_sk BIGINT,\n"
                        + "  d_date_id STRING,\n"
                        + "  d_date STRING,\n"
                        + "  d_month_seq BIGINT,\n"
                        + "  d_week_seq BIGINT,\n"
                        + "  d_quarter_seq BIGINT,\n"
                        + "  d_year BIGINT,\n"
                        + "  d_dow BIGINT,\n"
                        + "  d_moy BIGINT,\n"
                        + "  d_dom BIGINT,\n"
                        + "  d_qoy BIGINT,\n"
                        + "  d_fy_year BIGINT,\n"
                        + "  d_fy_quarter_seq BIGINT,\n"
                        + "  d_fy_week_seq BIGINT,\n"
                        + "  d_day_name STRING,\n"
                        + "  d_quarter_name STRING,\n"
                        + "  d_holiday STRING,\n"
                        + "  d_weekend STRING,\n"
                        + "  d_following_holiday STRING,\n"
                        + "  d_first_dom BIGINT,\n"
                        + "  d_last_dom BIGINT,\n"
                        + "  d_same_day_ly BIGINT,\n"
                        + "  d_same_day_lq BIGINT,\n"
                        + "  d_current_day STRING,\n"
                        + "  d_current_week STRING,\n"
                        + "  d_current_month STRING,\n"
                        + "  d_current_quarter STRING,\n"
                        + "  d_current_year STRING)\n"
                        + createTableFooter(tpcdsHome, "date_dim/"));
    }
}
