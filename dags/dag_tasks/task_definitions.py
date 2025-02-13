from ast import literal_eval
import json
import logging

import gdown
from airflow.providers.mysql.hooks.mysql import MySqlHook
import pandas as pd

from .utils import *


def extract_dataset() -> None:
    """
    Downloads a dataset from Google Drive and saves it as a JSON file.

    Raises:
        Exception: If the file download fails.
    """
    output = '/tmp/dataset.json'
    id = '1zMUaqu7065Qs_4wzPFlZEhMX3uBc35VY'

    try:
        gdown.download(id=id, output=output, fuzzy=True)
        logging.info(f"File downloaded successfully: {output}")
    except Exception as e:
        logging.error(f"Failed to download file: {e}")
        raise


def flatten_dataset() -> None:
    """
    Flattens a JSON file and saves it as a CSV file.

    Raises:
        Exception: If the file flattening fails.
    """
    input_path = '/tmp/dataset.json'
    output_path = '/tmp/flattened_dataset.csv'

    try:
        with open(input_path, 'r') as file:
            data = json.load(file)

        # Normalize and flatten the JSON data
        products_exploded_df = pd.json_normalize(data, sep="_").explode("products").reset_index(drop=True)
        products_flattened_df = pd.json_normalize(products_exploded_df["products"], sep="_").add_prefix("product_")
        reviews_exploded_df = pd.concat([products_exploded_df, products_flattened_df], axis=1).drop(columns=["products"]).explode("product_reviews").reset_index(drop=True)
        reviews_flattened_df = pd.json_normalize(reviews_exploded_df["product_reviews"], sep="_").add_prefix("product_review_")
        final_flattened_df = pd.concat([reviews_exploded_df, reviews_flattened_df], axis=1).drop(columns=["product_reviews"]).reset_index(drop=True)

        # Rename columns for clarity
        final_flattened_df.rename(columns={
            'id': 'order_id', 'total': 'order_total', 'discountedTotal': 'order_discountedTotal',
            'totalProducts': 'order_totalProducts', 'totalQuantity': 'order_totalQuantity'
        }, inplace=True)

        # Define the desired columns
        desired_columns = [
            'order_id', 'product_id', 'product_title', 'product_description', 'product_category', 'product_price', 'product_rating', 'product_stock', 'product_tags', 'product_brand', 'product_sku', 'product_weight', 
            'product_dimensions_width', 'product_dimensions_height', 'product_dimensions_depth', 'product_warrantyInformation', 'product_shippingInformation', 'product_availabilityStatus', 'product_review_rating', 
            'product_review_comment', 'product_review_date', 'product_review_reviewerName', 'product_review_reviewerEmail', 'product_returnPolicy', 'product_minimumOrderQuantity', 'product_meta_createdAt', 
            'product_meta_updatedAt', 'product_meta_barcode', 'product_meta_qrCode', 'product_images', 'product_thumbnail', 'product_quantity', 'product_total', 'product_discountedTotal', 'order_total', 'order_discountedTotal', 
            'order_totalProducts', 'order_totalQuantity', 'customer_id', 'customer_firstName', 'customer_lastName', 'customer_maidenName', 'customer_age', 'customer_gender', 'customer_email', 'customer_phone', 'customer_username', 'customer_password', 
            'customer_birthDate', 'customer_image', 'customer_bloodGroup', 'customer_height', 'customer_weight', 'customer_eyeColor', 'customer_hair_color', 'customer_hair_type', 'customer_ip', 'customer_address_address', 
            'customer_address_city', 'customer_address_state', 'customer_address_stateCode', 'customer_address_postalCode', 'customer_address_coordinates_lat', 'customer_address_coordinates_lng', 'customer_address_country', 
            'customer_macAddress', 'customer_university', 'customer_bank_cardExpire', 'customer_bank_cardNumber', 'customer_bank_cardType', 'customer_bank_currency', 'customer_bank_iban', 'customer_company_department', 
            'customer_company_name', 'customer_company_title', 'customer_company_address_address', 'customer_company_address_city', 'customer_company_address_state', 'customer_company_address_stateCode', 
            'customer_company_address_postalCode', 'customer_company_address_coordinates_lat', 'customer_company_address_coordinates_lng', 'customer_company_address_country', 'customer_ein', 'customer_ssn', 'customer_userAgent', 
            'customer_crypto_coin', 'customer_crypto_wallet', 'customer_crypto_network', 'customer_role'
        ]

        # Reindex and save the final DataFrame to CSV
        final_flattened_df = final_flattened_df.reindex(columns=desired_columns)
        final_flattened_df.to_csv(output_path, index=False)
        logging.info(f"File flattened and saved successfully: {output_path}")

    except Exception as e:
        logging.error(f"Failed to flatten file: {e}")
        raise


def transform_dim_category(**kwargs) -> None:
    """
    Extract distinct product categories and assign surrogate keys.
    Output CSV with columns: category_id, category_name
    """
    df = pd.read_csv('/tmp/flattened_dataset.csv')
    categories = df[['product_category']].drop_duplicates().reset_index(drop=True)
    categories['category_id'] = categories.index + 1
    categories = categories[['category_id', 'product_category']]
    categories.rename(columns={'product_category': 'category_name'}, inplace=True)
    categories.to_csv('/tmp/dim_category.csv', index=False)


def transform_dim_product_review(**kwargs) -> None:
    """
    Transform product review information.
    Rename review columns so that CSV columns exactly match the table:
    Expected output columns: product_id, customer_id, review_rating, review_comment,
        review_date, reviewer_name, reviewer_email
    """
    df = pd.read_csv('/tmp/flattened_dataset.csv')
    df_review = df[['product_id', 'product_review_rating', 'product_review_comment', 
                    'product_review_date', 'product_review_reviewerName', 'product_review_reviewerEmail']].drop_duplicates()
    df_review['review_id'] = df_review.reset_index().index + 1
    df_review = df_review[['review_id', 'product_id', 'product_review_rating', 'product_review_comment', 
                    'product_review_date', 'product_review_reviewerName', 'product_review_reviewerEmail']]
    df_review['product_review_date'] = pd.to_datetime(df_review['product_review_date']).dt.date
    df_review.rename(columns={
        'product_review_rating': 'review_rating',
        'product_review_comment': 'review_comment',
        'product_review_date': 'review_date',
        'product_review_reviewerName': 'reviewer_name',
        'product_review_reviewerEmail': 'reviewer_email'
    }, inplace=True)
    df_review.to_csv('/tmp/dim_product_review.csv', index=False)


def transform_dim_tag(**kwargs) -> None:
    """
    Parse the product_tags column, extract distinct tags, assign surrogate keys.
    Output CSV with columns: tag_id, tag_name
    """
    df = pd.read_csv('/tmp/flattened_dataset.csv')
    tags_series = df['product_tags'].dropna().apply(lambda x: literal_eval(x))
    all_tags = set()
    for tag_list in tags_series:
        for tag in tag_list:
            all_tags.add(tag.strip())
    df_tags = pd.DataFrame(list(all_tags), columns=['tag_name'])
    df_tags['tag_id'] = df_tags.index + 1
    df_tags = df_tags[['tag_id', 'tag_name']]
    df_tags.to_csv('/tmp/dim_tag.csv', index=False)


def transform_dim_product(**kwargs) -> None:
    """
    Transform product details. Join with dim_category (previously transformed) 
    to map product_category to surrogate category_id.
    Output CSV with columns: product_id, product_title, product_description,
        product_price, product_rating, product_stock, product_weight, product_sku, category_id
    """
    df = pd.read_csv('/tmp/flattened_dataset.csv')
    dim_category = pd.read_csv('/tmp/dim_category.csv')
    df_product = df[['product_id', 'product_title', 'product_description', 'product_price', 'product_rating', 'product_stock', 'product_weight',
                     'product_dimensions_height', 'product_dimensions_width', 'product_dimensions_depth', 'product_sku', 'product_category']].drop_duplicates()
    df_product = df_product.merge(dim_category, left_on='product_category', right_on='category_name', how='left')
    df_product.rename(columns={
                        'product_dimensions_height': 'product_height',
                        'product_dimensions_width': 'product_width',
                        'product_dimensions_depth': 'product_depth'
                    }, inplace=True)
    cols = ['product_id', 'product_title', 'product_description', 'product_price', 'product_rating', 'product_stock', 'product_weight',
            'product_height', 'product_width', 'product_depth', 'product_sku', 'category_id']
    df_product = df_product[cols]
    df_product.to_csv('/tmp/dim_product.csv', index=False)


def transform_bridge_product_tag(**kwargs) -> None:
    """
    For each product, expand the product_tags field and map tag names to surrogate tag_id.
    Output CSV with columns: product_id, tag_id
    """
    df = pd.read_csv('/tmp/flattened_dataset.csv')
    rows = []
    for _, row in df.iterrows():
        product_id = row['product_id']
        try:
            tags = literal_eval(row['product_tags'])
            for tag in tags:
                rows.append({'product_id': product_id, 'tag_name': tag.strip()})
        except Exception:
            continue
    df_bridge = pd.DataFrame(rows)
    dim_tag = pd.read_csv('/tmp/dim_tag.csv')
    df_bridge = df_bridge.merge(dim_tag, on='tag_name', how='left')
    df_bridge = df_bridge[['product_id', 'tag_id']].drop_duplicates()
    df_bridge.to_csv('/tmp/bridge_product_tag.csv', index=False)


def transform_dim_customer_demo(**kwargs) -> None:
    """
    Transform customer demographic info.
    Output CSV with columns: demographics_id, customer_birthdate, blood_group, eye_color, hair_color
    (Using customer_id as the surrogate demographics_id.)
    """
    df = pd.read_csv('/tmp/flattened_dataset.csv')
    df_demo = df[['customer_id', 'customer_birthDate', 'customer_gender', 'customer_company_title']].drop_duplicates()
    df_demo['customer_gender'] = df_demo['customer_gender'].str.capitalize()
    df_demo.rename(columns={'customer_birthDate': 'customer_birthdate', 'customer_company_title': 'customer_job'}, inplace=True)
    df_demo['demographics_id'] = df_demo['customer_id']
    df_demo = df_demo[['demographics_id', 'customer_birthdate', 'customer_gender', 'customer_job']]
    df_demo.to_csv('/tmp/dim_customer_demo.csv', index=False)


def transform_dim_address(**kwargs) -> None:
    """
    Transform customer address information.
    Output CSV with columns: address_id, address_line, city, state, postal_code, country,
        coordinates_lat, coordinates_lng
    (Using customer_id as the surrogate address_id.)
    """
    df = pd.read_csv('/tmp/flattened_dataset.csv')
    df_address = df[['customer_id', 'customer_address_address', 'customer_address_city', 
                        'customer_address_state', 'customer_address_postalCode', 'customer_address_country', 
                        'customer_address_coordinates_lat', 'customer_address_coordinates_lng']].drop_duplicates()
    df_address['address_id'] = df_address['customer_id']
    df_address.rename(columns={
        'customer_address_address': 'address_line',
        'customer_address_city': 'city',
        'customer_address_state': 'state',
        'customer_address_postalCode': 'postal_code',
        'customer_address_country': 'country',
        'customer_address_coordinates_lat': 'coordinates_lat',
        'customer_address_coordinates_lng': 'coordinates_lng'
    }, inplace=True)
    df_address = df_address[['address_id', 'address_line', 'city', 'state', 'postal_code', 'country', 'coordinates_lat', 'coordinates_lng']]
    df_address.to_csv('/tmp/dim_address.csv', index=False)


def transform_dim_customer(**kwargs) -> None:
    """
    Transform main customer data.
    Output CSV with columns: customer_id, first_name, last_name, email, demographics_id, address_id
    (Renaming customer_firstName, customer_lastName, customer_email to match table columns.)
    """
    df = pd.read_csv('/tmp/flattened_dataset.csv')
    df_customer = df[['customer_id', 'customer_firstName', 'customer_lastName', 'customer_email']].drop_duplicates()
    df_customer.rename(columns={
        'customer_firstName': 'first_name',
        'customer_lastName': 'last_name',
        'customer_email': 'email'
    }, inplace=True)
    # Use customer_id as surrogate for demographics_id and address_id
    df_customer['demographics_id'] = df_customer['customer_id']
    df_customer['address_id'] = df_customer['customer_id']
    df_customer = df_customer[['customer_id', 'first_name', 'last_name', 'email', 'demographics_id', 'address_id']]
    df_customer.to_csv('/tmp/dim_customer.csv', index=False)


def transform_fact_sales(**kwargs) -> None:
    """
    Transform sales facts.
    Map product_meta_createdAt to the surrogate date_id by joining with dim_date.
    Output CSV with columns: order_id, product_id, customer_id, date_id, quantity, sales_total, discount_total
    """
    df = pd.read_csv('/tmp/flattened_dataset.csv')
    df_fact = df[['order_id', 'product_id', 'customer_id', 'product_quantity', 'product_total', 'product_discountedTotal']].drop_duplicates()
    df_fact['sale_id'] = df_fact.reset_index().index + 1
    df_fact = df_fact[['sale_id', 'order_id', 'product_id', 'customer_id', 'product_quantity', 'product_total', 'product_discountedTotal']]
    df_fact.rename(columns={
        'product_quantity': 'quantity',
        'product_total': 'sales_total',
        'product_discountedTotal': 'discount_total'
    }, inplace=True)
    df_fact.to_csv('/tmp/fact_sales.csv', index=False)


def load_csv_to_mysql(csv_name: str, **kwargs) -> None:
    """
    Load a CSV file into a MySQL table.

    Args:
        csv_name (str): The name of the CSV file (without extension).

    Raises:
        Exception: If the loading process fails.
    """
    csv_path = f'/tmp/{csv_name}.csv'
    df = pd.read_csv(csv_path)

    # Construct column names and values
    columns = ', '.join(df.columns)
    values = []
    for row in df.itertuples(index=False, name=None):
        values.append(f"({', '.join(map(escape_value, row))})")

    # Construct the INSERT query (using ON DUPLICATE KEY UPDATE for upsert behavior)
    insert_query = f"""
        INSERT INTO {csv_name} ({columns})
        VALUES {', '.join(values)}
        ON DUPLICATE KEY UPDATE {df.columns[0]}={df.columns[0]};
    """

    # Use MySqlHook to run the query
    hook = MySqlHook(mysql_conn_id='dwh-conn')
    hook.run(insert_query)