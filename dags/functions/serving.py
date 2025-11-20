import pandas as pd
from tools.s3 import S3

from datetime import datetime, timezone, timedelta
from functions.batch import upload_to_s3

S3_BUCKET = "lambda-architecture"
s3 = S3(S3_BUCKET)


SERVING_PREFIX = "serving/"
SILVER_PREFIX = "batch/silver/"
GOLD_PREFIX = "batch/gold/"
REALTIME_PREFIX = "realtime/"

timestamp = datetime.now(timezone.utc).strftime("%Y-%m-%d_%H-%M-%S")
date_path = datetime.now(timezone.utc).strftime("%Y-%m-%d")


def load_all_csv(prefix):
    all_keys = s3.list_objects(prefix=prefix)
    csv_keys = [k for k in all_keys if k.endswith(".csv")]

    combined_dfs = []

    for key in csv_keys:
        obj = s3.get_object(key)
        if obj is None:
            continue
        df = pd.read_csv(obj['Body'])
        combined_dfs.append(df)

    if not combined_dfs:
        return pd.DataFrame()

    return pd.concat(combined_dfs, ignore_index=True)


def merge_iot_sensors():
    """Merge batch and realtime IoT sensor data"""
    print("Merging IoT sensors: Batch + Speed layers")

    # Load batch layer (silver - cleaned)
    batch_df = load_all_csv(
        f"{SILVER_PREFIX}iot_sensors/")

    # Load speed layer
    speed_df = load_all_csv(
        f"{REALTIME_PREFIX}iot_sensors/{date_path}/")

    frames = []

    if not batch_df.empty:
        batch_df['timestamp'] = pd.to_datetime(batch_df['timestamp'])
        batch_df['data_source'] = 'batch'
        frames.append(batch_df)
        print(f"Batch layer: {len(batch_df)} records")

    if not speed_df.empty:
        speed_df['timestamp'] = pd.to_datetime(speed_df['timestamp'])
        speed_df['data_source'] = 'speed'
        frames.append(speed_df)
        print(f"Speed layer: {len(speed_df)} records")

    if not frames:
        print("No IoT data to merge")
        return

    # Merge and deduplicate
    merged = pd.concat(frames, ignore_index=True)
    merged = merged.sort_values('timestamp')
    merged = merged.drop_duplicates(subset=['reading_id'], keep='last')

    # Add serving metadata
    merged['_served_at'] = pd.Timestamp.utcnow().isoformat()
    merged['_freshness_minutes'] = (
        pd.Timestamp.utcnow() - merged['timestamp']).dt.total_seconds() / 60

    # Calculate real-time building status
    latest_status = merged.groupby(['building_id', 'sensor_type']).agg({
        'value': 'last',
        'status': 'last',
        'battery_level': 'last',
        'timestamp': 'max'
    }).reset_index()

    # Write unified view
    key = f"{SERVING_PREFIX}unified/iot_sensors_unified_{timestamp}.csv"
    upload_to_s3(key, merged)

    # Write latest status for dashboards
    status_key = f"{SERVING_PREFIX}dashboards/iot_current_status.csv"
    upload_to_s3(status_key, latest_status)

    print(
        f"✓ IoT Serving: {len(merged)} total records, {len(latest_status)} current statuses")
    print(f"✓ Published to: s3://{S3_BUCKET}/{key}")


def merge_api_logs():
    """Merge batch and realtime API logs"""
    print("Merging API logs: Batch + Speed layers")

    batch_df = load_all_csv(
        S3_BUCKET, f"{SILVER_PREFIX}api_logs/")
    speed_df = load_all_csv(
        f"{REALTIME_PREFIX}api_logs/{date_path}/")

    frames = []

    if not batch_df.empty:
        batch_df['timestamp'] = pd.to_datetime(batch_df['timestamp'])
        batch_df['data_source'] = 'batch'
        frames.append(batch_df)

    if not speed_df.empty:
        speed_df['timestamp'] = pd.to_datetime(speed_df['timestamp'])
        speed_df['data_source'] = 'speed'
        frames.append(speed_df)

    if not frames:
        print("No API logs to merge")
        return

    merged = pd.concat(frames, ignore_index=True)
    merged = merged.sort_values('timestamp')
    merged = merged.drop_duplicates(subset=['log_id'], keep='last')

    merged['_served_at'] = pd.Timestamp.utcnow().isoformat()

    # Calculate real-time API health metrics (last hour)
    one_hour_ago = pd.Timestamp.utcnow() - timedelta(hours=1)
    recent = merged[merged['timestamp'] >= one_hour_ago].copy()

    if not recent.empty:
        api_health = recent.groupby('endpoint').agg({
            'log_id': 'count',
            'response_time_ms': 'mean',
            'is_error': lambda x: (x.sum() / len(x) * 100),
            'status_code': lambda x: x.mode()[0] if len(x) > 0 else None
        }).reset_index()
        api_health.columns = ['endpoint', 'request_count',
                              'avg_response_ms', 'error_rate_pct', 'common_status']

        health_key = f"{SERVING_PREFIX}dashboards/api_health_1h.csv"
        upload_to_s3(health_key, api_health)
        print(f"✓ API Health: {len(api_health)} endpoints monitored")

    # Write unified logs
    key = f"{SERVING_PREFIX}unified/api_logs_unified_{datetime.utcnow().strftime('%Y%m%d_%H%M%S')}.csv"
    upload_to_s3(key, merged)

    print(f"✓ API Serving: {len(merged)} total logs")


def merge_social_media():
    """Merge batch and realtime social media events"""
    print("Merging Social Media: Batch + Speed layers")

    batch_df = load_all_csv(
        f"{SILVER_PREFIX}social_media/")
    speed_df = load_all_csv(
        f"{REALTIME_PREFIX}social_media/{date_path}/")

    frames = []

    if not batch_df.empty:
        batch_df['timestamp'] = pd.to_datetime(batch_df['timestamp'])
        batch_df['data_source'] = 'batch'
        frames.append(batch_df)

    if not speed_df.empty:
        speed_df['timestamp'] = pd.to_datetime(speed_df['timestamp'])
        speed_df['data_source'] = 'speed'
        frames.append(speed_df)

    if not frames:
        print("No social media data to merge")
        return

    merged = pd.concat(frames, ignore_index=True)
    merged = merged.sort_values('timestamp')
    merged = merged.drop_duplicates(subset=['event_id'], keep='last')

    merged['_served_at'] = pd.Timestamp.utcnow().isoformat()

    # Real-time trending analysis (last 2 hours)
    two_hours_ago = pd.Timestamp.utcnow() - timedelta(hours=2)
    recent = merged[merged['timestamp'] >= two_hours_ago].copy()

    if not recent.empty:
        # Trending posts
        trending = recent[recent['engagement_score'] > 100].groupby('post_id').agg({
            'engagement_score': 'sum',
            'event_type': 'count',
            'platform': 'first',
            'timestamp': 'max'
        }).reset_index()
        trending = trending.sort_values(
            'engagement_score', ascending=False).head(50)
        trending.columns = ['post_id', 'total_engagement',
                            'event_count', 'platform', 'latest_activity']

        trend_key = f"{SERVING_PREFIX}dashboards/social_trending_2h.csv"
        upload_to_s3(trend_key, trending)
        print(f"✓ Trending: {len(trending)} viral posts identified")

    # Write unified events
    key = f"{SERVING_PREFIX}unified/social_media_unified_{timestamp}.csv"

    upload_to_s3(key, merged)

    print(f"✓ Social Serving: {len(merged)} total events")


def merge_ecommerce():
    """Merge batch and realtime e-commerce clickstream"""
    print("Merging E-Commerce: Batch + Speed layers")

    batch_df = load_all_csv(
        S3_BUCKET, f"{SILVER_PREFIX}ecommerce_clicks/")
    speed_df = load_all_csv(
        f"{REALTIME_PREFIX}ecommerce_clicks/{date_path}/")

    frames = []

    if not batch_df.empty:
        batch_df['timestamp'] = pd.to_datetime(batch_df['timestamp'])
        batch_df['data_source'] = 'batch'
        frames.append(batch_df)

    if not speed_df.empty:
        speed_df['timestamp'] = pd.to_datetime(speed_df['timestamp'])
        speed_df['data_source'] = 'speed'
        frames.append(speed_df)

    if not frames:
        print("No e-commerce data to merge")
        return

    merged = pd.concat(frames, ignore_index=True)
    merged = merged.sort_values('timestamp')
    merged = merged.drop_duplicates(subset=['event_id'], keep='last')

    merged['_served_at'] = pd.Timestamp.utcnow().isoformat()

    # Real-time conversion metrics (last hour)
    one_hour_ago = pd.Timestamp.utcnow() - timedelta(hours=1)
    recent = merged[merged['timestamp'] >= one_hour_ago].copy()

    if not recent.empty:
        # Active sessions and conversions
        session_metrics = recent.groupby('session_id').agg({
            'event_id': 'count',
            'event_type': lambda x: list(x),
            'timestamp': ['min', 'max']
        }).reset_index()

        # Count conversions
        conversions_1h = recent[recent['event_type']
                                == 'checkout_complete'].shape[0]
        cart_adds_1h = recent[recent['event_type'] == 'add_to_cart'].shape[0]

        # Calculate conversion rate
        if cart_adds_1h > 0:
            conversion_rate = (conversions_1h / cart_adds_1h) * 100
        else:
            conversion_rate = 0

        # Summary metrics
        summary = pd.DataFrame([{
            'timestamp': pd.Timestamp.utcnow().isoformat(),
            'active_sessions_1h': session_metrics.shape[0],
            'conversions_1h': conversions_1h,
            'cart_adds_1h': cart_adds_1h,
            'conversion_rate_pct': round(conversion_rate, 2),
            'total_events_1h': len(recent)
        }])

        summary_key = f"{SERVING_PREFIX}dashboards/ecommerce_realtime_kpi.csv"
        upload_to_s3(summary_key, summary)
        print(
            f"✓ E-Commerce KPI: {conversions_1h} conversions, {conversion_rate:.2f}% rate")

    # Write unified clickstream
    key = f"{SERVING_PREFIX}unified/ecommerce_unified_{datetime.utcnow().strftime('%Y%m%d_%H%M%S')}.csv"
    upload_to_s3(key, merged)

    print(f"✓ E-Commerce Serving: {len(merged)} total events")


def merge_financial():
    """Merge batch and realtime financial transactions"""
    print("Merging Financial Transactions: Batch + Speed layers")

    batch_df = load_all_csv(
        f"{SILVER_PREFIX}financial_transactions/")
    speed_df = load_all_csv(
        f"{REALTIME_PREFIX}financial_transactions/{date_path}/")

    frames = []

    if not batch_df.empty:
        batch_df['timestamp'] = pd.to_datetime(batch_df['timestamp'])
        batch_df['data_source'] = 'batch'
        frames.append(batch_df)

    if not speed_df.empty:
        speed_df['timestamp'] = pd.to_datetime(speed_df['timestamp'])
        speed_df['data_source'] = 'speed'
        frames.append(speed_df)

    if not frames:
        print("No financial data to merge")
        return

    merged = pd.concat(frames, ignore_index=True)
    merged = merged.sort_values('timestamp')
    merged = merged.drop_duplicates(subset=['transaction_id'], keep='last')

    merged['_served_at'] = pd.Timestamp.utcnow().isoformat()

    # Real-time fraud monitoring (last 15 minutes)
    fifteen_min_ago = pd.Timestamp.utcnow() - timedelta(minutes=15)
    recent = merged[merged['timestamp'] >= fifteen_min_ago].copy()

    if not recent.empty:
        # Fraud alerts
        fraud_alerts = recent[recent['fraud_flag'] == True].copy()
        fraud_alerts = fraud_alerts.sort_values('risk_score', ascending=False)

        # Transaction volume monitoring
        volume_summary = pd.DataFrame([{
            'timestamp': pd.Timestamp.utcnow().isoformat(),
            'transactions_15m': len(recent),
            'total_volume_15m': recent['amount'].sum(),
            'fraud_count_15m': len(fraud_alerts),
            'fraud_rate_pct': (len(fraud_alerts) / len(recent) * 100) if len(recent) > 0 else 0,
            'declined_count_15m': (recent['status'] == 'declined').sum(),
            'avg_risk_score': recent['risk_score'].mean()
        }])

        volume_key = f"{SERVING_PREFIX}dashboards/financial_realtime_monitor.csv"
        upload_to_s3(volume_key, volume_summary)

        if not fraud_alerts.empty:
            fraud_key = f"{SERVING_PREFIX}alerts/fraud_alerts_{timestamp}.csv"
            upload_to_s3(fraud_key, fraud_alerts)
            print(
                f"⚠️  FRAUD ALERT: {len(fraud_alerts)} suspicious transactions detected")

    # Write unified transactions
    key = f"{SERVING_PREFIX}unified/financial_unified_{timestamp}.csv"
    upload_to_s3(key, merged)

    print(f"✓ Financial Serving: {len(merged)} total transactions")


def publish_analytics_views(**context):
    # Load Gold layer metrics
    iot_gold = load_all_csv(
        f"{GOLD_PREFIX}fact_iot_daily/")
    api_gold = load_all_csv(
        f"{GOLD_PREFIX}fact_api_hourly/")
    social_gold = load_all_csv(
        f"{GOLD_PREFIX}fact_social_daily/")
    ecom_gold = load_all_csv(
        f"{GOLD_PREFIX}fact_ecommerce_revenue/")
    fin_gold = load_all_csv(
        f"{GOLD_PREFIX}fact_financial_daily/")

    # Create master analytics summary
    summary_data = []

    if not iot_gold.empty:
        summary_data.append({
            'domain': 'IoT Sensors',
            'total_records': len(iot_gold),
            'latest_date': iot_gold['date'].max() if 'date' in iot_gold.columns else None
        })

    if not api_gold.empty:
        summary_data.append({
            'domain': 'API Logs',
            'total_records': len(api_gold),
            'latest_date': api_gold['date'].max() if 'date' in api_gold.columns else None
        })

    if not social_gold.empty:
        summary_data.append({
            'domain': 'Social Media',
            'total_records': len(social_gold),
            'latest_date': social_gold['date'].max() if 'date' in social_gold.columns else None
        })

    if not ecom_gold.empty:
        summary_data.append({
            'domain': 'E-Commerce',
            'total_records': len(ecom_gold),
            'latest_date': ecom_gold['date'].max() if 'date' in ecom_gold.columns else None
        })

    if not fin_gold.empty:
        summary_data.append({
            'domain': 'Financial',
            'total_records': len(fin_gold),
            'latest_date': fin_gold['date'].max() if 'date' in fin_gold.columns else None
        })

    if summary_data:
        summary_df = pd.DataFrame(summary_data)
        summary_df['published_at'] = datetime.utcnow().isoformat()

        summary_key = f"{SERVING_PREFIX}analytics/master_summary.csv"

        upload_to_s3(summary_key, summary_df)
        print(
            f"✓ Published master analytics summary: {len(summary_data)} domains")

    print("✓ SERVING LAYER COMPLETE: All views published")
