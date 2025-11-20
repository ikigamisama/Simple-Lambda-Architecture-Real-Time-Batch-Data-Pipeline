import pandas as pd
from datafakegenerator import DataFakeGenerator
from tools.s3 import S3
from io import BytesIO
from datetime import datetime, timezone

import random

S3_BUCKET = "lambda-architecture"
s3 = S3(S3_BUCKET)
PIPELINE_VERSION = 'v1.0'

hashtags_pool = [
    "#datascience", "#ai", "#ml", "#tech", "#coding", "#python",
    "#analytics", "#bigdata", "#cloud", "#devops", "#portfolio",
    "#deeplearning", "#machinelearning", "#dataengineer", "#dataengineering",
    "#datapipeline", "#spark", "#airflow", "#docker", "#kubernetes",
    "#cloudcomputing", "#aws", "#azure", "#gcp", "#mle",
    "#programming", "#100daysofcode", "#softwareengineering", "#computerscience",
    "#career", "#projects", "#dbt", "#snowflake", "#datawarehouse",
    "#etl", "#elt", "#datalake", "#dataanalytics", "#pytorch",
    "#tensorflow", "#sql", "#postgres", "#duckdb", "#minio",
    "#businessintelligence", "#dataquality", "#dataops"
]

hashtag_groups = [
    " ".join(random.sample(hashtags_pool, random.randint(1, 10)))
    for _ in range(5)
]


schema_io_sensor = [
    {
        "label": "reading_id",
        "key_label": "uuid_v4",
        "group": 'it',
        "options": {"blank_percentage": 0}
    },
    {
        "label": "sensor_id",
        "key_label": "character_sequence",
        "group": 'advanced',
        "options": {"blank_percentage": 0, "format": "SENSOR_####"}
    },
    {
        "label": "building_id",
        "key_label": "character_sequence",
        "group": 'advanced',
        "options": {"blank_percentage": 0, "format": "BUILDING_^"}
    },
    {
        "label": "floor",
        "key_label": "number",
        "group": 'basic',
        "options": {"blank_percentage": 0, 'min': 1, "max": 40, "decimal": 0}
    },
    {
        'label': 'sensor_type',
        "key_label": 'sensor_type',
        "group": "it",
        "options": {"blank_percentage": 0}
    },
    {
        'label': 'value',
        "key_label": 'number',
        "group": "basic",
        "options": {"blank_percentage": 0, 'min': 0, "max": 100, }
    },
    {
        'label': 'unit',
        "key_label": 'custom_list',
        "group": "basic",
        "options": {"blank_percentage": 0, "custom_format": "percent, count"}
    },
    {
        'label': 'status',
        "key_label": 'custom_list',
        "group": "basic",
        "options": {"blank_percentage": 0, "custom_format": "normal, warning, calibrating, critical"}
    },
    {
        'label': 'battery_level',
        "key_label": 'number',
        "group": "basic",
        "options": {"blank_percentage": 0, 'min': 0, "max": 100}
    },
]

schema_api_access = [
    {
        "label": "log_id",
        "key_label": "uuid_v4",
        "group": 'it',
        "options": {"blank_percentage": 0}
    },
    {
        "label": "request_id",
        "key_label": "character_sequence",
        "group": 'advanced',
        "options": {"blank_percentage": 0, "format": "REQ-%%%%-%%%"}
    },
    {
        "label": "method",
        "key_label": "http_method",
        "group": 'it',
        "options": {"blank_percentage": 0}
    },
    {
        "label": "endpoint",
        "key_label": "api_endpoint_path",
        "group": 'it',
        "options": {"blank_percentage": 0}
    },
    {
        "label": "status_code",
        "key_label": "http_status_code",
        "group": 'it',
        "options": {"blank_percentage": 0}
    },
    {
        'label': 'response_time_ms',
        "key_label": 'number',
        "group": "basic",
        "options": {"blank_percentage": 0, 'min': 0, "max": 1000}
    },
    {
        'label': 'client_ip',
        "key_label": 'ip_address_v4',
        "group": "it",
        "options": {"blank_percentage": 0}
    },
    {
        'label': 'user_agent',
        "key_label": 'user_agent',
        "group": "it",
        "options": {"blank_percentage": 0}
    },
    {
        'label': 'region',
        "key_label": 'data_center',
        "group": "it",
        "options": {"blank_percentage": 0}
    },
    {
        "label": "api_key",
        "key_label": "api_key",
        "group": 'it',
        "options": {"blank_percentage": 0, "prefix": "API-"}
    },
    {
        "label": "api_key_id",
        "key_label": "character_sequence",
        "group": 'advanced',
        "options": {"blank_percentage": 0, "format": "API_%%%%_%%%%_%%%%"}
    },
    {
        'label': 'bytes_sent',
        "key_label": 'number',
        "group": "basic",
        "options": {"blank_percentage": 0, 'min': 50, "max": 50000}
    },
    {
        'label': 'bytes_received',
        "key_label": 'number',
        "group": "basic",
        "options": {"blank_percentage": 0, 'min': 100, "max": 10000}
    },

]


schema_social_media = [
    {
        "label": "event_id",
        "key_label": "uuid_v4",
        "group": 'it',
        "options": {"blank_percentage": 0}
    },
    {
        "label": "user_id",
        "key_label": "character_sequence",
        "group": 'advanced',
        "options": {"blank_percentage": 0, "format": "USER_#####"}
    },
    {
        "label": "post_id",
        "key_label": "character_sequence",
        "group": 'advanced',
        "options": {"blank_percentage": 0, "format": "POST_########"}
    },
    {
        "label": "event_type",
        "key_label": 'custom_list',
        "group": "basic",
        "options": {"blank_percentage": 0, "custom_format": "post,like,comment,share,follow,view"}
    },
    {
        "label": "platform",
        "key_label": 'custom_list',
        "group": "basic",
        "options": {"blank_percentage": 0, "custom_format": "facebook,instaream,x,linkdein,tiktok"}
    },
    {
        "label": "content_type",
        "key_label": 'custom_list',
        "group": "basic",
        "options": {"blank_percentage": 0, "custom_format": "text,image,video,story,reel,carousel"}
    },
    {
        'label': 'engagement_score',
        "key_label": 'number',
        "group": "basic",
        "options": {"blank_percentage": 0, 'min': 0, "max": 100}
    },
    {
        'label': 'character_count',
        "key_label": 'number',
        "group": "basic",
        "options": {"blank_percentage": 0, 'min': 0, "max": 100}
    },
    {
        "label": "has_media",
        "key_label": 'custom_list',
        "group": "basic",
        "options": {"blank_percentage": 0, "custom_format": "image,video,reel,carousel"}
    },
    {
        "label": "hashtags",
        "key_label": 'custom_list',
        "group": "basic",
        "options": {"blank_percentage": 0, "custom_format": ','.join(hashtag_groups)}
    },
    {
        "label": "is_verified",
        "key_label": 'boolean',
        "group": "basic",
        "options": {"blank_percentage": 0}
    },
    {
        "label": "device_type",
        "key_label": 'form_factor',
        "group": "it",
        "options": {"blank_percentage": 0}
    },
    {
        "label": "location_country",
        "key_label": 'country_code',
        "group": "location",
        "options": {"blank_percentage": 0}
    },
]

schema_e_commerce = [
    {
        "label": "event_id",
        "key_label": "uuid_v4",
        "group": 'it',
        "options": {"blank_percentage": 0}
    },
    {
        "label": "session_id",
        "key_label": "character_sequence",
        "group": 'advanced',
        "options": {"blank_percentage": 0, "format": "SESSIOn_#####"}
    },
    {
        "label": "user_id",
        "key_label": "character_sequence",
        "group": 'advanced',
        "options": {"blank_percentage": 0, "format": "USER_#####"}
    },
    {
        "label": "anonymous_id",
        "key_label": "uuid_v1",
        "group": 'it',
        "options": {"blank_percentage": 0}
    },
    {
        "label": "event_type",
        "key_label": 'custom_list',
        "group": "basic",
        "options": {"blank_percentage": 0, "custom_format": "product_view,add_to_cart,remove_from_cart,checkout,search,wishlist"}
    },
    {
        "label": "page_url",
        "key_label": 'custom_list',
        "group": "basic",
        "options": {"blank_percentage": 0, "custom_format": "/,/products,/cart,/checkout,/search,/account,/wishlist,/orders"}
    },
    {
        "label": "product_id",
        "key_label": "character_sequence",
        "group": 'advanced',
        "options": {"blank_percentage": 0, "format": "PROD_#####"}
    },
    {
        "label": "product_category",
        "key_label": 'custom_list',
        "group": "basic",
        "options": {"blank_percentage": 0, "custom_format": "Electronics,Fashion,Garden,Sports,Books,Beauty,Toys,Automotivbe,Hardware"}
    },
    {
        "label": "price",
        "key_label": 'money',
        "group": "commerce",
        "options": {"blank_percentage": 0, "min": 9.99, "max": 9999.99}
    },
    {
        "label": "quantity",
        "key_label": 'number',
        "group": "basic",
        "options": {"blank_percentage": 0, "min": 1, "max": 10}
    },
    {
        "label": "device_type",
        "key_label": 'form_factor',
        "group": "it",
        "options": {"blank_percentage": 0}
    },
    {
        "label": "browser",
        "key_label": 'browser',
        "group": "it",
        "options": {"blank_percentage": 0}
    },
    {
        "label": "os",
        "key_label": 'operating_system',
        "group": "it",
        "options": {"blank_percentage": 0}
    },
    {
        "label": "traffic_source",
        "key_label": 'custom_list',
        "group": "basic",
        "options": {"blank_percentage": 0, "custom_format": "search,social_media,email,referral,display_ads"}
    },
    {
        "label": "is_new_user",
        "key_label": 'boolean',
        "group": "basic",
        "options": {"blank_percentage": 0}
    },
    {
        "label": "session_duration_sec",
        "key_label": 'number',
        "group": "basic",
        "options": {"blank_percentage": 0, "min": 10, "max": 3600}
    },
    {
        "label": "page_load_time_ms",
        "key_label": 'number',
        "group": "basic",
        "options": {"blank_percentage": 0, "min": 200, "max": 300}
    },
    {
        "label": "country",
        "key_label": 'country',
        "group": "location",
        "options": {"blank_percentage": 0}
    },
    {
        "label": "city",
        "key_label": 'city',
        "group": "location",
        "options": {"blank_percentage": 0}
    },
]


schema_financial_transactions = [
    {
        "label": "transaction_id",
        "key_label": "uuid_v4",
        "group": 'it',
        "options": {"blank_percentage": 0}
    },
    {
        "label": "account_id",
        "key_label": "character_sequence",
        "group": 'advanced',
        "options": {"blank_percentage": 0, "format": "ACC_#####_#####"}
    },
    {
        "label": "customer_id",
        "key_label": "character_sequence",
        "group": 'advanced',
        "options": {"blank_percentage": 0, "format": "CUST_#####_#####"}
    },
    {
        "label": "transaction_type",
        "key_label": 'custom_list',
        "group": "basic",
        "options": {"blank_percentage": 0, "custom_format": "purchase,withdrawal,deposit,transfer,refund,payment"}
    },
    {
        "label": "amount",
        "key_label": 'number',
        "group": "basic",
        "options": {"blank_percentage": 0, "min": 10, "max": 10000}
    },
    {
        "label": "currency",
        "key_label": 'currency_code',
        "group": "commerce",
        "options": {"blank_percentage": 0, "min": 10, "max": 10000}
    },
    {
        "label": "payment_method",
        "key_label": 'payment_method',
        "group": "commerce",
        "options": {"blank_percentage": 0, }
    },
    {
        "label": "merchant_id",
        "key_label": "character_sequence",
        "group": 'advanced',
        "options": {"blank_percentage": 0, "format": "MERCH_####"}
    },
    {
        "label": "merchant_category",
        "key_label": 'custom_list',
        "group": "basic",
        "options": {"blank_percentage": 0, "custom_format": "Retail,Grocery,Gas Station,Restaurant,Online Shopping,Entertainment,Healthcare,Travel,Utilities"}
    },
    {
        "label": "merchant_country",
        "key_label": 'country_code',
        "group": "location",
        "options": {"blank_percentage": 0}
    },
    {
        "label": "customer_country",
        "key_label": 'country_code',
        "group": "location",
        "options": {"blank_percentage": 0}
    },
    {
        "label": "status",
        "key_label": 'custom_list',
        "group": "basic",
        "options": {"blank_percentage": 0, "custom_format": "Decline,Approved,Pending"}
    },
    {
        "label": "is_international",
        "key_label": 'boolean',
        "group": "basic",
        "options": {"blank_percentage": 0}
    },
    {
        "label": "is_online",
        "key_label": 'boolean',
        "group": "basic",
        "options": {"blank_percentage": 0}
    },
    {
        "label": "risk_score",
        "key_label": 'number',
        "group": "basic",
        "options": {"blank_percentage": 0, "min": 1, "max": 100}
    },
    {
        "label": "fraud_flag",
        "key_label": 'boolean',
        "group": "basic",
        "options": {"blank_percentage": 0}
    },
    {
        "label": "processing_time_ms",
        "key_label": 'number',
        "group": "basic",
        "options": {"blank_percentage": 0, "min": 10, "max": 3600}
    },
    {
        "label": "device_fingerprint",
        "key_label": "uuid_v1",
        "group": 'it',
        "options": {"blank_percentage": 0}
    },
    {
        'label': 'ip_address',
        "key_label": 'ip_address_v4',
        "group": "it",
        "options": {"blank_percentage": 0}
    },
    {
        "label": "auth_method",
        "key_label": 'custom_list',
        "group": "basic",
        "options": {"blank_percentage": 0, "custom_format": "PIN,Signature,Biometric,2FA,None"}
    },

]


def to_df(n, schema):
    data = DataFakeGenerator(schema).many(n).data
    return pd.DataFrame([{**row, "timestamp": datetime.now(timezone.utc).isoformat()} for row in data])


def upload_to_s3(category, df):
    timestamp = datetime.now(timezone.utc).strftime("%Y-%m-%d_%H-%M-%S")
    date_path = datetime.now(timezone.utc).strftime("%Y-%m-%d")

    filename = f"realtime/{category}/{date_path}/{category}_{timestamp}.csv"

    csv_bytes = df.to_csv(index=False).encode("utf-8")

    s3.write_file(
        object_name=filename,
        data=csv_bytes,
        contentType="text/csv"
    )

    return filename


def ingest_iot_sensors():
    df = to_df(random.randint(150, 500), schema_io_sensor)

    df['_ingest_timestamp'] = datetime.now(timezone.utc).isoformat()
    df['_pipeline_version'] = PIPELINE_VERSION

    file_name = upload_to_s3('iot_sensors', df)

    print(f"✓ Ingested {len(df)} sensor readings")
    print(f"✓ Written to: s3://{S3_BUCKET}/{file_name}")
    print(f"✓ Sensor types: {df['sensor_type'].value_counts().to_dict()}")
    print(f"✓ Buildings covered: {df['building_id'].nunique()}")


def ingest_api_logs():
    df = to_df(random.randint(250, 750), schema_api_access)

    df['_ingest_timestamp'] = datetime.utcnow().isoformat()
    df['_source'] = datetime.now(timezone.utc).isoformat()
    df['_pipeline_version'] = PIPELINE_VERSION

    file_name = upload_to_s3('api_logs', df)

    error_rate = (df['status_code'] >= 400).sum() / len(df) * 100

    print(f"✓ Ingested {len(df)} API log entries")
    print(f"✓ Written to: s3://{S3_BUCKET}/{file_name}")
    print(f"✓ Error rate: {error_rate:.2f}%")
    print(f"✓ Avg response time: {df['response_time_ms'].mean():.0f}ms")
    print(
        f"✓ Top endpoints: {df['endpoint'].value_counts().head(3).to_dict()}")


def ingest_social_media():
    df = to_df(random.randint(350, 1000), schema_social_media)

    df['_ingest_timestamp'] = datetime.utcnow().isoformat()
    df['_pipeline_stage'] = 'ingestion'
    df['_pipeline_version'] = PIPELINE_VERSION

    file_name = upload_to_s3('social_media', df)

    total_engagement = df['engagement_score'].sum()
    viral_posts = (df['engagement_score'] > 500).sum()

    print(f"✓ Ingested {len(df)} social media events")
    print(f"✓ Written to: s3://{S3_BUCKET}/{file_name}")
    print(f"✓ Total engagement: {total_engagement:,}")
    print(f"✓ Viral posts (>500 engagement): {viral_posts}")
    print(f"✓ Events by platform: {df['platform'].value_counts().to_dict()}")
    print(f"✓ Events by type: {df['event_type'].value_counts().to_dict()}")


def ingest_clickstream():
    df = to_df(random.randint(400, 800), schema_e_commerce)

    df['_ingest_timestamp'] = datetime.utcnow().isoformat()
    df['_data_source'] = 'web_analytics'
    df['_pipeline_version'] = PIPELINE_VERSION

    file_name = upload_to_s3('ecommerce_clicks', df)

    conversion_events = df[df['event_type'] == 'checkout_complete'].shape[0]
    cart_adds = df[df['event_type'] == 'add_to_cart'].shape[0]
    total_revenue = df[df['event_type'] == 'checkout_complete']['price'].sum()

    print(f"✓ Ingested {len(df)} clickstream events")
    print(f"✓ Written to: s3://{S3_BUCKET}/{file_name}")
    print(f"✓ Conversions: {conversion_events}")
    print(f"✓ Cart additions: {cart_adds}")
    print(f"✓ Revenue generated: ${total_revenue:,.2f}")
    print(f"✓ Events by type: {df['event_type'].value_counts().to_dict()}")
    print(
        f"✓ Traffic sources: {df['traffic_source'].value_counts().to_dict()}")


def ingest_financial_transactions():
    df = to_df(random.randint(250, 500), schema_financial_transactions)

    df['_ingest_timestamp'] = datetime.utcnow().isoformat()
    df['_compliance_checked'] = True
    df['_data_classification'] = 'PCI-DSS'
    df['_pipeline_version'] = PIPELINE_VERSION

    file_name = upload_to_s3('financial_transactions', df)

    total_volume = df['amount'].sum()
    approved_trans = df[df['status'] == 'approved'].shape[0]
    declined_trans = df[df['status'] == 'declined'].shape[0]
    fraud_detected = df[df['fraud_flag'] == True].shape[0]
    avg_risk = df['risk_score'].mean()

    print(f"✓ Ingested {len(df)} financial transactions")
    print(f"✓ Written to: s3://{S3_BUCKET}/{file_name}")
    print(f"✓ Total transaction volume: ${total_volume:,.2f}")
    print(f"✓ Approved: {approved_trans} | Declined: {declined_trans}")
    print(
        f"✓ Fraud flags: {fraud_detected} ({fraud_detected/len(df)*100:.1f}%)")
    print(f"✓ Average risk score: {avg_risk:.2f}")
    print(
        f"✓ Transaction types: {df['transaction_type'].value_counts().to_dict()}")
