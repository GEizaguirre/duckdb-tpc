#!/usr/bin/env python3
"""
Script to translate TPC-H/TPC-DS queries to Spark SQL and print their physical plans.

Usage:
    python explain_query.py <benchmark> <query_number>
    
Arguments:
    benchmark: 'tpc-h' or 'tpc-ds'
    query_number: The query number to explain

Example:
    python explain_query.py tpc-h 1
    python explain_query.py tpc-ds 42
"""

import sys
import os
import sqlglot
from pyspark.sql import SparkSession


def get_query_path(benchmark: str, query_number: int) -> str:
    """Get the path to the SQL file for the given benchmark and query number."""
    script_dir = os.path.dirname(os.path.abspath(__file__))

    if benchmark == "tpc-h":
        return os.path.join(script_dir, "queries-tpc-h", f"{query_number}.sql")
    elif benchmark == "tpc-ds":
        return os.path.join(script_dir, "queries-tpc-ds", f"query{query_number}.sql")
    else:
        raise ValueError(f"Unknown benchmark: {benchmark}. Use 'tpc-h' or 'tpc-ds'.")


def read_query(filepath: str) -> str:
    """Read the SQL query from a file."""
    if not os.path.exists(filepath):
        raise FileNotFoundError(f"Query file not found: {filepath}")

    with open(filepath, 'r') as f:
        return f.read()


def translate_to_spark(query: str, source_dialect: str = "duckdb") -> str:
    """Translate a SQL query to Spark SQL using sqlglot."""
    try:
        translated = sqlglot.transpile(query, read=source_dialect, write="spark", pretty=True)
        return "\n".join(translated)
    except Exception as e:
        raise RuntimeError(f"Failed to translate query: {e}")


def get_physical_plan(spark: SparkSession, query: str) -> str:
    """Get the physical plan for a Spark SQL query."""
    try:
        df = spark.sql(query)
        return df._jdf.queryExecution().explainString(
            spark._jvm.org.apache.spark.sql.execution.ExplainMode.fromString("extended")
        )
    except Exception as e:
        # If execution fails, try to at least get the parsed/analyzed plan
        raise RuntimeError(f"Failed to get physical plan: {e}")


def create_spark_session() -> SparkSession:
    """Create a Spark session for query planning."""
    return SparkSession.builder \
        .appName("TPC Query Explainer") \
        .master("local[*]") \
        .config("spark.sql.catalogImplementation", "in-memory") \
        .config("spark.driver.memory", "2g") \
        .getOrCreate()


def create_tpc_h_tables(spark: SparkSession):
    """Create empty TPC-H tables with the correct schema for query planning."""
    # TPC-H table schemas
    spark.sql("""
        CREATE TABLE IF NOT EXISTS nation (
            n_nationkey INT,
            n_name STRING,
            n_regionkey INT,
            n_comment STRING
        )
    """)
    
    spark.sql("""
        CREATE TABLE IF NOT EXISTS region (
            r_regionkey INT,
            r_name STRING,
            r_comment STRING
        )
    """)
    
    spark.sql("""
        CREATE TABLE IF NOT EXISTS part (
            p_partkey INT,
            p_name STRING,
            p_mfgr STRING,
            p_brand STRING,
            p_type STRING,
            p_size INT,
            p_container STRING,
            p_retailprice DECIMAL(15,2),
            p_comment STRING
        )
    """)
    
    spark.sql("""
        CREATE TABLE IF NOT EXISTS supplier (
            s_suppkey INT,
            s_name STRING,
            s_address STRING,
            s_nationkey INT,
            s_phone STRING,
            s_acctbal DECIMAL(15,2),
            s_comment STRING
        )
    """)
    
    spark.sql("""
        CREATE TABLE IF NOT EXISTS partsupp (
            ps_partkey INT,
            ps_suppkey INT,
            ps_availqty INT,
            ps_supplycost DECIMAL(15,2),
            ps_comment STRING
        )
    """)
    
    spark.sql("""
        CREATE TABLE IF NOT EXISTS customer (
            c_custkey INT,
            c_name STRING,
            c_address STRING,
            c_nationkey INT,
            c_phone STRING,
            c_acctbal DECIMAL(15,2),
            c_mktsegment STRING,
            c_comment STRING
        )
    """)
    
    spark.sql("""
        CREATE TABLE IF NOT EXISTS orders (
            o_orderkey INT,
            o_custkey INT,
            o_orderstatus STRING,
            o_totalprice DECIMAL(15,2),
            o_orderdate DATE,
            o_orderpriority STRING,
            o_clerk STRING,
            o_shippriority INT,
            o_comment STRING
        )
    """)
    
    spark.sql("""
        CREATE TABLE IF NOT EXISTS lineitem (
            l_orderkey INT,
            l_partkey INT,
            l_suppkey INT,
            l_linenumber INT,
            l_quantity DECIMAL(15,2),
            l_extendedprice DECIMAL(15,2),
            l_discount DECIMAL(15,2),
            l_tax DECIMAL(15,2),
            l_returnflag STRING,
            l_linestatus STRING,
            l_shipdate DATE,
            l_commitdate DATE,
            l_receiptdate DATE,
            l_shipinstruct STRING,
            l_shipmode STRING,
            l_comment STRING
        )
    """)


def create_tpc_ds_tables(spark: SparkSession):
    """Create empty TPC-DS tables with the correct schema for query planning."""
    # TPC-DS has many tables - creating the most commonly used ones
    spark.sql("""
        CREATE TABLE IF NOT EXISTS store_sales (
            ss_sold_date_sk INT,
            ss_sold_time_sk INT,
            ss_item_sk INT,
            ss_customer_sk INT,
            ss_cdemo_sk INT,
            ss_hdemo_sk INT,
            ss_addr_sk INT,
            ss_store_sk INT,
            ss_promo_sk INT,
            ss_ticket_number INT,
            ss_quantity INT,
            ss_wholesale_cost DECIMAL(7,2),
            ss_list_price DECIMAL(7,2),
            ss_sales_price DECIMAL(7,2),
            ss_ext_discount_amt DECIMAL(7,2),
            ss_ext_sales_price DECIMAL(7,2),
            ss_ext_wholesale_cost DECIMAL(7,2),
            ss_ext_list_price DECIMAL(7,2),
            ss_ext_tax DECIMAL(7,2),
            ss_coupon_amt DECIMAL(7,2),
            ss_net_paid DECIMAL(7,2),
            ss_net_paid_inc_tax DECIMAL(7,2),
            ss_net_profit DECIMAL(7,2)
        )
    """)
    
    spark.sql("""
        CREATE TABLE IF NOT EXISTS store_returns (
            sr_returned_date_sk INT,
            sr_return_time_sk INT,
            sr_item_sk INT,
            sr_customer_sk INT,
            sr_cdemo_sk INT,
            sr_hdemo_sk INT,
            sr_addr_sk INT,
            sr_store_sk INT,
            sr_reason_sk INT,
            sr_ticket_number INT,
            sr_return_quantity INT,
            sr_return_amt DECIMAL(7,2),
            sr_return_tax DECIMAL(7,2),
            sr_return_amt_inc_tax DECIMAL(7,2),
            sr_fee DECIMAL(7,2),
            sr_return_ship_cost DECIMAL(7,2),
            sr_refunded_cash DECIMAL(7,2),
            sr_reversed_charge DECIMAL(7,2),
            sr_store_credit DECIMAL(7,2),
            sr_net_loss DECIMAL(7,2)
        )
    """)
    
    spark.sql("""
        CREATE TABLE IF NOT EXISTS catalog_sales (
            cs_sold_date_sk INT,
            cs_sold_time_sk INT,
            cs_ship_date_sk INT,
            cs_bill_customer_sk INT,
            cs_bill_cdemo_sk INT,
            cs_bill_hdemo_sk INT,
            cs_bill_addr_sk INT,
            cs_ship_customer_sk INT,
            cs_ship_cdemo_sk INT,
            cs_ship_hdemo_sk INT,
            cs_ship_addr_sk INT,
            cs_call_center_sk INT,
            cs_catalog_page_sk INT,
            cs_ship_mode_sk INT,
            cs_warehouse_sk INT,
            cs_item_sk INT,
            cs_promo_sk INT,
            cs_order_number INT,
            cs_quantity INT,
            cs_wholesale_cost DECIMAL(7,2),
            cs_list_price DECIMAL(7,2),
            cs_sales_price DECIMAL(7,2),
            cs_ext_discount_amt DECIMAL(7,2),
            cs_ext_sales_price DECIMAL(7,2),
            cs_ext_wholesale_cost DECIMAL(7,2),
            cs_ext_list_price DECIMAL(7,2),
            cs_ext_tax DECIMAL(7,2),
            cs_coupon_amt DECIMAL(7,2),
            cs_ext_ship_cost DECIMAL(7,2),
            cs_net_paid DECIMAL(7,2),
            cs_net_paid_inc_tax DECIMAL(7,2),
            cs_net_paid_inc_ship DECIMAL(7,2),
            cs_net_paid_inc_ship_tax DECIMAL(7,2),
            cs_net_profit DECIMAL(7,2)
        )
    """)
    
    spark.sql("""
        CREATE TABLE IF NOT EXISTS catalog_returns (
            cr_returned_date_sk INT,
            cr_returned_time_sk INT,
            cr_item_sk INT,
            cr_refunded_customer_sk INT,
            cr_refunded_cdemo_sk INT,
            cr_refunded_hdemo_sk INT,
            cr_refunded_addr_sk INT,
            cr_returning_customer_sk INT,
            cr_returning_cdemo_sk INT,
            cr_returning_hdemo_sk INT,
            cr_returning_addr_sk INT,
            cr_call_center_sk INT,
            cr_catalog_page_sk INT,
            cr_ship_mode_sk INT,
            cr_warehouse_sk INT,
            cr_reason_sk INT,
            cr_order_number INT,
            cr_return_quantity INT,
            cr_return_amount DECIMAL(7,2),
            cr_return_tax DECIMAL(7,2),
            cr_return_amt_inc_tax DECIMAL(7,2),
            cr_fee DECIMAL(7,2),
            cr_return_ship_cost DECIMAL(7,2),
            cr_refunded_cash DECIMAL(7,2),
            cr_reversed_charge DECIMAL(7,2),
            cr_store_credit DECIMAL(7,2),
            cr_net_loss DECIMAL(7,2)
        )
    """)
    
    spark.sql("""
        CREATE TABLE IF NOT EXISTS web_sales (
            ws_sold_date_sk INT,
            ws_sold_time_sk INT,
            ws_ship_date_sk INT,
            ws_item_sk INT,
            ws_bill_customer_sk INT,
            ws_bill_cdemo_sk INT,
            ws_bill_hdemo_sk INT,
            ws_bill_addr_sk INT,
            ws_ship_customer_sk INT,
            ws_ship_cdemo_sk INT,
            ws_ship_hdemo_sk INT,
            ws_ship_addr_sk INT,
            ws_web_page_sk INT,
            ws_web_site_sk INT,
            ws_ship_mode_sk INT,
            ws_warehouse_sk INT,
            ws_promo_sk INT,
            ws_order_number INT,
            ws_quantity INT,
            ws_wholesale_cost DECIMAL(7,2),
            ws_list_price DECIMAL(7,2),
            ws_sales_price DECIMAL(7,2),
            ws_ext_discount_amt DECIMAL(7,2),
            ws_ext_sales_price DECIMAL(7,2),
            ws_ext_wholesale_cost DECIMAL(7,2),
            ws_ext_list_price DECIMAL(7,2),
            ws_ext_tax DECIMAL(7,2),
            ws_coupon_amt DECIMAL(7,2),
            ws_ext_ship_cost DECIMAL(7,2),
            ws_net_paid DECIMAL(7,2),
            ws_net_paid_inc_tax DECIMAL(7,2),
            ws_net_paid_inc_ship DECIMAL(7,2),
            ws_net_paid_inc_ship_tax DECIMAL(7,2),
            ws_net_profit DECIMAL(7,2)
        )
    """)
    
    spark.sql("""
        CREATE TABLE IF NOT EXISTS web_returns (
            wr_returned_date_sk INT,
            wr_returned_time_sk INT,
            wr_item_sk INT,
            wr_refunded_customer_sk INT,
            wr_refunded_cdemo_sk INT,
            wr_refunded_hdemo_sk INT,
            wr_refunded_addr_sk INT,
            wr_returning_customer_sk INT,
            wr_returning_cdemo_sk INT,
            wr_returning_hdemo_sk INT,
            wr_returning_addr_sk INT,
            wr_web_page_sk INT,
            wr_reason_sk INT,
            wr_order_number INT,
            wr_return_quantity INT,
            wr_return_amt DECIMAL(7,2),
            wr_return_tax DECIMAL(7,2),
            wr_return_amt_inc_tax DECIMAL(7,2),
            wr_fee DECIMAL(7,2),
            wr_return_ship_cost DECIMAL(7,2),
            wr_refunded_cash DECIMAL(7,2),
            wr_reversed_charge DECIMAL(7,2),
            wr_account_credit DECIMAL(7,2),
            wr_net_loss DECIMAL(7,2)
        )
    """)
    
    spark.sql("""
        CREATE TABLE IF NOT EXISTS inventory (
            inv_date_sk INT,
            inv_item_sk INT,
            inv_warehouse_sk INT,
            inv_quantity_on_hand INT
        )
    """)
    
    spark.sql("""
        CREATE TABLE IF NOT EXISTS store (
            s_store_sk INT,
            s_store_id STRING,
            s_rec_start_date DATE,
            s_rec_end_date DATE,
            s_closed_date_sk INT,
            s_store_name STRING,
            s_number_employees INT,
            s_floor_space INT,
            s_hours STRING,
            s_manager STRING,
            s_market_id INT,
            s_geography_class STRING,
            s_market_desc STRING,
            s_market_manager STRING,
            s_division_id INT,
            s_division_name STRING,
            s_company_id INT,
            s_company_name STRING,
            s_street_number STRING,
            s_street_name STRING,
            s_street_type STRING,
            s_suite_number STRING,
            s_city STRING,
            s_county STRING,
            s_state STRING,
            s_zip STRING,
            s_country STRING,
            s_gmt_offset DECIMAL(5,2),
            s_tax_precentage DECIMAL(5,2)
        )
    """)
    
    spark.sql("""
        CREATE TABLE IF NOT EXISTS customer (
            c_customer_sk INT,
            c_customer_id STRING,
            c_current_cdemo_sk INT,
            c_current_hdemo_sk INT,
            c_current_addr_sk INT,
            c_first_shipto_date_sk INT,
            c_first_sales_date_sk INT,
            c_salutation STRING,
            c_first_name STRING,
            c_last_name STRING,
            c_preferred_cust_flag STRING,
            c_birth_day INT,
            c_birth_month INT,
            c_birth_year INT,
            c_birth_country STRING,
            c_login STRING,
            c_email_address STRING,
            c_last_review_date_sk INT
        )
    """)
    
    spark.sql("""
        CREATE TABLE IF NOT EXISTS customer_address (
            ca_address_sk INT,
            ca_address_id STRING,
            ca_street_number STRING,
            ca_street_name STRING,
            ca_street_type STRING,
            ca_suite_number STRING,
            ca_city STRING,
            ca_county STRING,
            ca_state STRING,
            ca_zip STRING,
            ca_country STRING,
            ca_gmt_offset DECIMAL(5,2),
            ca_location_type STRING
        )
    """)
    
    spark.sql("""
        CREATE TABLE IF NOT EXISTS customer_demographics (
            cd_demo_sk INT,
            cd_gender STRING,
            cd_marital_status STRING,
            cd_education_status STRING,
            cd_purchase_estimate INT,
            cd_credit_rating STRING,
            cd_dep_count INT,
            cd_dep_employed_count INT,
            cd_dep_college_count INT
        )
    """)
    
    spark.sql("""
        CREATE TABLE IF NOT EXISTS date_dim (
            d_date_sk INT,
            d_date_id STRING,
            d_date DATE,
            d_month_seq INT,
            d_week_seq INT,
            d_quarter_seq INT,
            d_year INT,
            d_dow INT,
            d_moy INT,
            d_dom INT,
            d_qoy INT,
            d_fy_year INT,
            d_fy_quarter_seq INT,
            d_fy_week_seq INT,
            d_day_name STRING,
            d_quarter_name STRING,
            d_holiday STRING,
            d_weekend STRING,
            d_following_holiday STRING,
            d_first_dom INT,
            d_last_dom INT,
            d_same_day_ly INT,
            d_same_day_lq INT,
            d_current_day STRING,
            d_current_week STRING,
            d_current_month STRING,
            d_current_quarter STRING,
            d_current_year STRING
        )
    """)
    
    spark.sql("""
        CREATE TABLE IF NOT EXISTS household_demographics (
            hd_demo_sk INT,
            hd_income_band_sk INT,
            hd_buy_potential STRING,
            hd_dep_count INT,
            hd_vehicle_count INT
        )
    """)
    
    spark.sql("""
        CREATE TABLE IF NOT EXISTS item (
            i_item_sk INT,
            i_item_id STRING,
            i_rec_start_date DATE,
            i_rec_end_date DATE,
            i_item_desc STRING,
            i_current_price DECIMAL(7,2),
            i_wholesale_cost DECIMAL(7,2),
            i_brand_id INT,
            i_brand STRING,
            i_class_id INT,
            i_class STRING,
            i_category_id INT,
            i_category STRING,
            i_manufact_id INT,
            i_manufact STRING,
            i_size STRING,
            i_formulation STRING,
            i_color STRING,
            i_units STRING,
            i_container STRING,
            i_manager_id INT,
            i_product_name STRING
        )
    """)
    
    spark.sql("""
        CREATE TABLE IF NOT EXISTS promotion (
            p_promo_sk INT,
            p_promo_id STRING,
            p_start_date_sk INT,
            p_end_date_sk INT,
            p_item_sk INT,
            p_cost DECIMAL(15,2),
            p_response_target INT,
            p_promo_name STRING,
            p_channel_dmail STRING,
            p_channel_email STRING,
            p_channel_catalog STRING,
            p_channel_tv STRING,
            p_channel_radio STRING,
            p_channel_press STRING,
            p_channel_event STRING,
            p_channel_demo STRING,
            p_channel_details STRING,
            p_purpose STRING,
            p_discount_active STRING
        )
    """)
    
    spark.sql("""
        CREATE TABLE IF NOT EXISTS time_dim (
            t_time_sk INT,
            t_time_id STRING,
            t_time INT,
            t_hour INT,
            t_minute INT,
            t_second INT,
            t_am_pm STRING,
            t_shift STRING,
            t_sub_shift STRING,
            t_meal_time STRING
        )
    """)
    
    spark.sql("""
        CREATE TABLE IF NOT EXISTS warehouse (
            w_warehouse_sk INT,
            w_warehouse_id STRING,
            w_warehouse_name STRING,
            w_warehouse_sq_ft INT,
            w_street_number STRING,
            w_street_name STRING,
            w_street_type STRING,
            w_suite_number STRING,
            w_city STRING,
            w_county STRING,
            w_state STRING,
            w_zip STRING,
            w_country STRING,
            w_gmt_offset DECIMAL(5,2)
        )
    """)
    
    spark.sql("""
        CREATE TABLE IF NOT EXISTS web_site (
            web_site_sk INT,
            web_site_id STRING,
            web_rec_start_date DATE,
            web_rec_end_date DATE,
            web_name STRING,
            web_open_date_sk INT,
            web_close_date_sk INT,
            web_class STRING,
            web_manager STRING,
            web_mkt_id INT,
            web_mkt_class STRING,
            web_mkt_desc STRING,
            web_market_manager STRING,
            web_company_id INT,
            web_company_name STRING,
            web_street_number STRING,
            web_street_name STRING,
            web_street_type STRING,
            web_suite_number STRING,
            web_city STRING,
            web_county STRING,
            web_state STRING,
            web_zip STRING,
            web_country STRING,
            web_gmt_offset DECIMAL(5,2),
            web_tax_percentage DECIMAL(5,2)
        )
    """)
    
    spark.sql("""
        CREATE TABLE IF NOT EXISTS web_page (
            wp_web_page_sk INT,
            wp_web_page_id STRING,
            wp_rec_start_date DATE,
            wp_rec_end_date DATE,
            wp_creation_date_sk INT,
            wp_access_date_sk INT,
            wp_autogen_flag STRING,
            wp_customer_sk INT,
            wp_url STRING,
            wp_type STRING,
            wp_char_count INT,
            wp_link_count INT,
            wp_image_count INT,
            wp_max_ad_count INT
        )
    """)
    
    spark.sql("""
        CREATE TABLE IF NOT EXISTS catalog_page (
            cp_catalog_page_sk INT,
            cp_catalog_page_id STRING,
            cp_start_date_sk INT,
            cp_end_date_sk INT,
            cp_department STRING,
            cp_catalog_number INT,
            cp_catalog_page_number INT,
            cp_description STRING,
            cp_type STRING
        )
    """)
    
    spark.sql("""
        CREATE TABLE IF NOT EXISTS call_center (
            cc_call_center_sk INT,
            cc_call_center_id STRING,
            cc_rec_start_date DATE,
            cc_rec_end_date DATE,
            cc_closed_date_sk INT,
            cc_open_date_sk INT,
            cc_name STRING,
            cc_class STRING,
            cc_employees INT,
            cc_sq_ft INT,
            cc_hours STRING,
            cc_manager STRING,
            cc_mkt_id INT,
            cc_mkt_class STRING,
            cc_mkt_desc STRING,
            cc_market_manager STRING,
            cc_division INT,
            cc_division_name STRING,
            cc_company INT,
            cc_company_name STRING,
            cc_street_number STRING,
            cc_street_name STRING,
            cc_street_type STRING,
            cc_suite_number STRING,
            cc_city STRING,
            cc_county STRING,
            cc_state STRING,
            cc_zip STRING,
            cc_country STRING,
            cc_gmt_offset DECIMAL(5,2),
            cc_tax_percentage DECIMAL(5,2)
        )
    """)
    
    spark.sql("""
        CREATE TABLE IF NOT EXISTS income_band (
            ib_income_band_sk INT,
            ib_lower_bound INT,
            ib_upper_bound INT
        )
    """)
    
    spark.sql("""
        CREATE TABLE IF NOT EXISTS reason (
            r_reason_sk INT,
            r_reason_id STRING,
            r_reason_desc STRING
        )
    """)
    
    spark.sql("""
        CREATE TABLE IF NOT EXISTS ship_mode (
            sm_ship_mode_sk INT,
            sm_ship_mode_id STRING,
            sm_type STRING,
            sm_code STRING,
            sm_carrier STRING,
            sm_contract STRING
        )
    """)


def main():
    if len(sys.argv) != 3:
        print(__doc__)
        sys.exit(1)
    
    benchmark = sys.argv[1].lower()
    try:
        query_number = int(sys.argv[2])
    except ValueError:
        print(f"Error: Query number must be an integer, got '{sys.argv[2]}'")
        sys.exit(1)
    
    if benchmark not in ["tpc-h", "tpc-ds"]:
        print(f"Error: Benchmark must be 'tpc-h' or 'tpc-ds', got '{benchmark}'")
        sys.exit(1)
    
    # Get and read the query
    query_path = get_query_path(benchmark, query_number)
    print(f"Reading query from: {query_path}")
    
    try:
        original_query = read_query(query_path)
    except FileNotFoundError as e:
        print(f"Error: {e}")
        sys.exit(1)
    
    print("\n" + "=" * 60)
    print("ORIGINAL QUERY:")
    print("=" * 60)
    print(original_query)
    
    # Translate to Spark SQL
    print("\n" + "=" * 60)
    print("TRANSLATED TO SPARK SQL:")
    print("=" * 60)
    
    try:
        spark_query = translate_to_spark(original_query)
        print(spark_query)
    except RuntimeError as e:
        print(f"Error: {e}")
        sys.exit(1)
    
    # Create Spark session and get physical plan
    print("\n" + "=" * 60)
    print("SPARK PHYSICAL PLAN:")
    print("=" * 60)
    
    spark = None
    try:
        spark = create_spark_session()
        
        # Create the appropriate tables
        if benchmark == "tpc-h":
            create_tpc_h_tables(spark)
        else:
            create_tpc_ds_tables(spark)
        
        # Get and print the physical plan
        plan = get_physical_plan(spark, spark_query)
        print(plan)
        
    except Exception as e:
        print(f"Error getting physical plan: {e}")
        sys.exit(1)
    finally:
        if spark:
            spark.stop()


if __name__ == "__main__":
    main()
