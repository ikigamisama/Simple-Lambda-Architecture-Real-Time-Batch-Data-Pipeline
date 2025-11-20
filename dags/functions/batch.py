import pandas as pd
from tools.s3 import S3

from datetime import datetime, timezone

S3_BUCKET = "lambda-architecture"
s3 = S3(S3_BUCKET)

BRONZE_PREFIX = "batch/bronze/"
SILVER_PREFIX = "batch/silver/"
GOLD_PREFIX = "batch/gold/"

timestamp = datetime.now(timezone.utc).strftime("%Y-%m-%d_%H-%M-%S")
date_path = datetime.now(timezone.utc).strftime("%Y-%m-%d")


def upload_to_s3(filename, df):
    csv_bytes = df.to_csv(index=False).encode("utf-8")
    s3.write_file(
        object_name=filename,
        data=csv_bytes,
        contentType="text/csv"
    )


def read_csv_under_prefix(filename):
    if not s3.object_exists(filename):
        print(f"File not found: {filename}")
        return pd.DataFrame()

    obj = s3.get_object(filename)
    df = pd.read_csv(obj['Body'])
    return df


def bronze_layer():
    streams = {
        "iot_sensors": "iot_sensors/",
        "api_logs": "api_logs/",
        "social_media": "social_media/",
        "ecommerce_clicks": "ecommerce_clicks/",
        "financial_transactions": "financial_transactions/"
    }

    stats = {}

    for stream_name, stream_path in streams.items():
        print(f"Processing {stream_name} stream...")
        prefix = f"realtime/{stream_path}{date_path}/"
        all_keys = s3.list_objects(prefix=prefix)

        csv_keys = [k for k in all_keys if k.endswith(".csv")]

        if not csv_keys:
            print(f"No CSVs found for {stream_name} in {prefix}")
            stats[stream_name] = 0
            continue

        combined_dfs = []

        for key in csv_keys:
            obj = s3.get_object(key)
            if obj is None:
                continue
            df = pd.read_csv(obj['Body'])
            combined_dfs.append(df)

        if not combined_dfs:
            stats[stream_name] = 0
            continue

        df = pd.concat(combined_dfs, ignore_index=True)

        # Add bronze metadata
        df['_bronze_ingested_at'] = datetime.utcnow().isoformat()
        df['_bronze_batch_date'] = date_path
        df['_record_count'] = len(df)

        # Write to bronze with date partition
        filename = f"{BRONZE_PREFIX}{stream_name}/{stream_name}_{date_path}.csv"
        upload_to_s3(filename, df)

        stats[stream_name] = len(df)
        print(f"✓ Bronze {stream_name}: {len(df)} records → {filename}")

    print(
        f"✓ BRONZE COMPLETE: {sum(stats.values())} total records across {len(stats)} streams")


def silver_layer():
    stats = {}

    iot_df = read_csv_under_prefix(
        f"{BRONZE_PREFIX}iot_sensors/iot_sensors_{date_path}.csv")
    if not iot_df.empty:
        original_count = len(iot_df)

        # Deduplication
        iot_df = iot_df.drop_duplicates(subset=['reading_id'], keep='last')

        # Type enforcement
        iot_df['value'] = pd.to_numeric(iot_df['value'], errors='coerce')
        iot_df['battery_level'] = pd.to_numeric(
            iot_df['battery_level'], errors='coerce')
        iot_df['floor'] = pd.to_numeric(
            iot_df['floor'], errors='coerce').astype('Int64')

        # Data quality: remove invalid readings
        iot_df = iot_df[iot_df['value'].notna()]
        iot_df = iot_df[iot_df['battery_level'] >= 5]  # Low battery threshold

        # Standardize timestamps
        iot_df['timestamp'] = pd.to_datetime(
            iot_df['timestamp'], errors='coerce')
        iot_df = iot_df[iot_df['timestamp'].notna()]

        # Add silver metadata
        iot_df['_silver_processed_at'] = datetime.utcnow().isoformat()
        iot_df['_quality_score'] = 100 - \
            ((original_count - len(iot_df)) / original_count * 100)

        stats['iot_sensors'] = {'cleaned': len(
            iot_df), 'removed': original_count - len(iot_df)}

        filename = f"{SILVER_PREFIX}iot_sensors/iot_sensors_clean_{date_path}.csv"
        upload_to_s3(filename, iot_df)

    api_df = read_csv_under_prefix(
        f"{BRONZE_PREFIX}api_logs/api_logs_{date_path}.csv")
    if not api_df.empty:
        original_count = len(api_df)

        # Deduplication
        api_df = api_df.drop_duplicates(subset=['log_id'], keep='last')

        # Type enforcement
        api_df['status_code'] = pd.to_numeric(
            api_df['status_code'], errors='coerce').astype('Int64')
        api_df['response_time_ms'] = pd.to_numeric(
            api_df['response_time_ms'], errors='coerce')
        api_df['bytes_sent'] = pd.to_numeric(
            api_df['bytes_sent'], errors='coerce').astype('Int64')

        # Calculate derived fields
        api_df['is_error'] = api_df['status_code'] >= 400
        api_df['is_slow'] = api_df['response_time_ms'] > 1000
        api_df['response_category'] = api_df['status_code'].apply(
            lambda x: f"{x // 100}xx" if pd.notna(x) else 'unknown'
        )

        # Standardize timestamps
        api_df['timestamp'] = pd.to_datetime(
            api_df['timestamp'], errors='coerce')
        api_df = api_df[api_df['timestamp'].notna()]

        # Add silver metadata
        api_df['_silver_processed_at'] = datetime.utcnow().isoformat()

        filename = f"{SILVER_PREFIX}api_logs/api_logs_clean_{date_path}.csv"
        upload_to_s3(filename, api_df)

        stats['api_logs'] = {'cleaned': len(
            api_df), 'removed': original_count - len(api_df)}
        print(
            f"✓ Silver API: {len(api_df)} records (removed {original_count - len(api_df)})")

    social_df = read_csv_under_prefix(
        f"{BRONZE_PREFIX}social_media/social_media_{date_path}.csv")
    if not social_df.empty:
        original_count = len(social_df)

        # Deduplication
        social_df = social_df.drop_duplicates(subset=['event_id'], keep='last')

        # Type enforcement
        social_df['engagement_score'] = pd.to_numeric(
            social_df['engagement_score'], errors='coerce').astype('Int64')
        social_df['character_count'] = pd.to_numeric(
            social_df['character_count'], errors='coerce').astype('Int64')

        # Calculate engagement tiers
        social_df['engagement_tier'] = pd.cut(
            social_df['engagement_score'],
            bins=[0, 10, 50, 100, 500, float('inf')],
            labels=['low', 'medium', 'high', 'viral', 'mega_viral']
        )

        # Standardize timestamps
        social_df['timestamp'] = pd.to_datetime(
            social_df['timestamp'], errors='coerce')
        social_df = social_df[social_df['timestamp'].notna()]

        # Add silver metadata
        social_df['_silver_processed_at'] = datetime.utcnow().isoformat()

        filename = f"{SILVER_PREFIX}social_media/social_media_clean_{date_path}.csv"
        upload_to_s3(filename, social_df)

        stats['social_media'] = {'cleaned': len(
            social_df), 'removed': original_count - len(social_df)}
        print(
            f"✓ Silver Social: {len(social_df)} records (removed {original_count - len(social_df)})")

    ecom_df = read_csv_under_prefix(
        f"{BRONZE_PREFIX}ecommerce_clicks/ecommerce_clicks_{date_path}.csv")
    if not ecom_df.empty:
        original_count = len(ecom_df)

        # Deduplication
        ecom_df = ecom_df.drop_duplicates(subset=['event_id'], keep='last')

        # Type enforcement
        ecom_df['price'] = pd.to_numeric(ecom_df['price'], errors='coerce')
        ecom_df['quantity'] = pd.to_numeric(
            ecom_df['quantity'], errors='coerce').astype('Int64')
        ecom_df['session_duration_sec'] = pd.to_numeric(
            ecom_df['session_duration_sec'], errors='coerce').astype('Int64')
        ecom_df['page_load_time_ms'] = pd.to_numeric(
            ecom_df['page_load_time_ms'], errors='coerce').astype('Int64')

        # Calculate cart value
        ecom_df['cart_value'] = ecom_df['price'] * \
            ecom_df['quantity'].fillna(1)

        # Categorize events into funnel stages
        ecom_df['funnel_stage'] = ecom_df['event_type'].map({
            'page_view': 'awareness',
            'product_view': 'interest',
            'add_to_cart': 'consideration',
            'checkout_start': 'intent',
            'checkout_complete': 'conversion',
            'search': 'awareness',
            'filter_apply': 'interest',
            'wishlist_add': 'consideration'
        }).fillna('other')

        # Standardize timestamps
        ecom_df['timestamp'] = pd.to_datetime(
            ecom_df['timestamp'], errors='coerce')
        ecom_df = ecom_df[ecom_df['timestamp'].notna()]

        # Add silver metadata
        ecom_df['_silver_processed_at'] = datetime.utcnow().isoformat()

        filename = f"{SILVER_PREFIX}ecommerce_clicks/ecommerce_clean_{date_path}.csv"
        upload_to_s3(filename, ecom_df)

        stats['ecommerce_clicks'] = {'cleaned': len(
            ecom_df), 'removed': original_count - len(ecom_df)}
        print(
            f"✓ Silver E-Commerce: {len(ecom_df)} records (removed {original_count - len(ecom_df)})")

    fin_df = read_csv_under_prefix(
        f"{BRONZE_PREFIX}financial_transactions/financial_transactions_{date_path}.csv")
    if not fin_df.empty:
        original_count = len(fin_df)

        # Deduplication
        fin_df = fin_df.drop_duplicates(subset=['transaction_id'], keep='last')

        # Type enforcement
        fin_df['amount'] = pd.to_numeric(fin_df['amount'], errors='coerce')
        fin_df['risk_score'] = pd.to_numeric(
            fin_df['risk_score'], errors='coerce')
        fin_df['processing_time_ms'] = pd.to_numeric(
            fin_df['processing_time_ms'], errors='coerce').astype('Int64')

        # Remove invalid amounts
        fin_df = fin_df[fin_df['amount'] > 0]

        # Categorize risk levels
        fin_df['risk_level'] = pd.cut(
            fin_df['risk_score'],
            bins=[0, 30, 60, 80, 100],
            labels=['low', 'medium', 'high', 'critical']
        )

        # Standardize timestamps
        fin_df['timestamp'] = pd.to_datetime(
            fin_df['timestamp'], errors='coerce')
        fin_df = fin_df[fin_df['timestamp'].notna()]

        # Add silver metadata
        fin_df['_silver_processed_at'] = datetime.utcnow().isoformat()

        filename = f"{SILVER_PREFIX}financial_transactions/financial_clean_{date_path}.csv"
        upload_to_s3(filename, fin_df)

        stats['financial_transactions'] = {'cleaned': len(
            fin_df), 'removed': original_count - len(fin_df)}
    print(
        f"✓ Silver Financial: {len(fin_df)} records (removed {original_count - len(fin_df)})")


def gold_layer():
    print(f"Processing gold layer for {date_path}")

    # ==================== IoT Analytics ====================
    iot_silver = read_csv_under_prefix(
        f"{SILVER_PREFIX}iot_sensors/iot_sensors_clean_{date_path}.csv")
    if not iot_silver.empty:
        iot_silver['date'] = pd.to_datetime(iot_silver['timestamp']).dt.date

        # Daily sensor aggregates by building and sensor type
        iot_agg = iot_silver.groupby(['date', 'building_id', 'sensor_type']).agg(
            total_readings=('reading_id', 'count'),
            avg_value=('value', 'mean'),
            min_value=('value', 'min'),
            max_value=('value', 'max'),
            avg_battery=('battery_level', 'mean'),
            warning_count=('status', lambda x: (x == 'warning').sum()),
            unique_sensors=('sensor_id', 'nunique')
        ).reset_index()

        key = f"{GOLD_PREFIX}fact_iot_daily/iot_daily_{date_path}.csv"
        upload_to_s3(key, iot_agg)
        print(f"✓ Gold IoT: {len(iot_agg)} daily aggregates")

    # ==================== API Performance Metrics ====================
    api_silver = read_csv_under_prefix(
        f"{SILVER_PREFIX}api_logs/api_logs_clean_{date_path}.csv")
    if not api_silver.empty:
        api_silver['date'] = pd.to_datetime(api_silver['timestamp']).dt.date
        api_silver['hour'] = pd.to_datetime(api_silver['timestamp']).dt.hour

        # Hourly API metrics by endpoint
        api_agg = api_silver.groupby(['date', 'hour', 'endpoint']).agg(
            total_requests=('log_id', 'count'),
            avg_response_time=('response_time_ms', 'mean'),
            p95_response_time=('response_time_ms', lambda x: x.quantile(0.95)),
            error_count=('is_error', 'sum'),
            error_rate=('is_error', lambda x: x.sum() / len(x) * 100),
            slow_requests=('is_slow', 'sum'),
            total_bytes_sent=('bytes_sent', 'sum'),
            unique_api_keys=('api_key_id', 'nunique')
        ).reset_index()

        key = f"{GOLD_PREFIX}fact_api_hourly/api_hourly_{date_path}.csv"
        upload_to_s3(key, api_agg)
        print(f"✓ Gold API: {len(api_agg)} hourly metrics")

    # ==================== Social Media Engagement ====================
    social_silver = read_csv_under_prefix(
        f"{SILVER_PREFIX}social_media/social_media_clean_{date_path}.csv")
    if not social_silver.empty:
        social_silver['date'] = pd.to_datetime(
            social_silver['timestamp']).dt.date

        # Daily engagement by platform and event type
        social_agg = social_silver.groupby(['date', 'platform', 'event_type']).agg(
            total_events=('event_id', 'count'),
            total_engagement=('engagement_score', 'sum'),
            avg_engagement=('engagement_score', 'mean'),
            viral_posts=('engagement_tier', lambda x: (
                x == 'viral').sum() + (x == 'mega_viral').sum()),
            unique_users=('user_id', 'nunique'),
            verified_users=('is_verified', 'sum'),
            mobile_percentage=('device_type', lambda x: (
                x == 'mobile').sum() / len(x) * 100)
        ).reset_index()

        key = f"{GOLD_PREFIX}fact_social_daily/social_daily_{date_path}.csv"
        upload_to_s3(key, social_agg)
        print(f"✓ Gold Social: {len(social_agg)} daily engagement metrics")

    # ==================== E-Commerce Conversion Funnel ====================
    ecom_silver = read_csv_under_prefix(
        f"{SILVER_PREFIX}ecommerce_clicks/ecommerce_clean_{date_path}.csv")
    if not ecom_silver.empty:
        ecom_silver['date'] = pd.to_datetime(ecom_silver['timestamp']).dt.date

        # Daily funnel metrics
        funnel_agg = ecom_silver.groupby(['date', 'funnel_stage']).agg(
            total_events=('event_id', 'count'),
            unique_sessions=('session_id', 'nunique'),
            unique_users=('user_id', 'nunique'),
            avg_session_duration=('session_duration_sec', 'mean'),
            new_users=('is_new_user', 'sum')
        ).reset_index()

        key = f"{GOLD_PREFIX}fact_ecommerce_funnel/ecom_funnel_{date_path}.csv"
        upload_to_s3(key, funnel_agg)

        # Daily revenue metrics
        revenue_df = ecom_silver[ecom_silver['event_type']
                                 == 'checkout_complete'].copy()
        if not revenue_df.empty:
            revenue_agg = revenue_df.groupby(['date', 'product_category']).agg(
                total_orders=('event_id', 'count'),
                total_revenue=('cart_value', 'sum'),
                avg_order_value=('cart_value', 'mean'),
                unique_customers=('user_id', 'nunique')
            ).reset_index()

            key = f"{GOLD_PREFIX}fact_ecommerce_revenue/ecom_revenue_{date_path}.csv"
            upload_to_s3(key, revenue_agg)
            print(f"✓ Gold E-Commerce: Revenue and funnel metrics")

    # ==================== Financial Transaction Analytics ====================
    fin_silver = read_csv_under_prefix(
        f"{SILVER_PREFIX}financial_transactions/financial_clean_{date_path}.csv")
    if not fin_silver.empty:
        fin_silver['date'] = pd.to_datetime(fin_silver['timestamp']).dt.date

        # Daily transaction metrics
        fin_agg = fin_silver.groupby(['date', 'transaction_type', 'status']).agg(
            total_transactions=('transaction_id', 'count'),
            total_volume=('amount', 'sum'),
            avg_amount=('amount', 'mean'),
            avg_risk_score=('risk_score', 'mean'),
            fraud_count=('fraud_flag', 'sum'),
            fraud_rate=('fraud_flag', lambda x: x.sum() / len(x) * 100),
            avg_processing_time=('processing_time_ms', 'mean'),
            unique_customers=('customer_id', 'nunique')
        ).reset_index()

        key = f"{GOLD_PREFIX}fact_financial_daily/financial_daily_{date_path}.csv"
        upload_to_s3(key, fin_agg)

        # Fraud detection summary
        fraud_summary = fin_silver.groupby(['date', 'risk_level']).agg(
            transaction_count=('transaction_id', 'count'),
            flagged_count=('fraud_flag', 'sum'),
            declined_count=('status', lambda x: (x == 'declined').sum()),
            total_amount=('amount', 'sum')
        ).reset_index()

        key = f"{GOLD_PREFIX}fact_fraud_summary/fraud_summary_{date_path}.csv"
        upload_to_s3(key, fraud_summary)
        print(f"✓ Gold Financial: Transaction and fraud metrics")

    print(f"✓ GOLD COMPLETE: Business metrics ready for analytics")
