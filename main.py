from flask import Flask, jsonify
import os
from datetime import datetime, timedelta
import pandas as pd
import requests
from io import StringIO
from google.cloud import bigquery

app = Flask(__name__)

class AxonDataCollector:
    def __init__(self):
        self.api_key = os.environ['AXON_API_KEY']
        self.project_id = os.environ['GCP_PROJECT_ID']
        self.dataset_id = os.environ['BQ_DATASET_ID']
        self.bq_client = bigquery.Client(project=self.project_id)
    
    def fetch_user_level_data(self, date, platform, application):
        """User-Level Ad Revenue API 호출"""
        print(f"  📥 User-Level 데이터 조회: {application} ({platform})")
        
        url = "https://r.applovin.com/max/userAdRevenueReport"
        params = {
            'api_key': self.api_key,
            'date': date,
            'platform': platform,
            'application': application,
            'aggregated': 'false'
        }
        
        try:
            response = requests.get(url, params=params, timeout=60)
            response.raise_for_status()
            data = response.json()
            
            csv_url = data.get('ad_revenue_report_url') or data.get('url')
            if not csv_url:
                print(f"    ⚠️ CSV URL 없음")
                return None
            
            csv_response = requests.get(csv_url, timeout=120)
            csv_response.raise_for_status()
            
            df = pd.read_csv(StringIO(csv_response.text))
            if len(df) == 0:
                print(f"    ⚠️ 데이터 없음")
                return None
            
            # 컬럼 매핑
            column_mapping = {
                'Date': 'impression_timestamp',
                'Ad Unit ID': 'ad_unit_id',
                'Ad Unit Name': 'ad_unit_name',
                'Waterfall': 'waterfall',
                'Ad Format': 'ad_format',
                'Placement': 'placement',
                'Ad Placement': 'ad_placement',
                'Network': 'network',
                'Country': 'country',
                'Device Type': 'device_type',
                'IDFA': 'idfa',
                'IDFV': 'idfv',
                'User ID': 'user_id',
                'Custom Data': 'custom_data',
                'Revenue': 'revenue'
            }
            df.rename(columns=column_mapping, inplace=True)
            
            # 필수 컬럼 추가
            df['report_date'] = pd.to_datetime(date).date()
            df['application'] = application
            df['package_name'] = application
            df['platform'] = platform
            df['loaded_at'] = datetime.utcnow()
            
            if 'impression_timestamp' in df.columns:
                df['impression_timestamp'] = pd.to_datetime(df['impression_timestamp'])
            if 'revenue' in df.columns:
                df['revenue'] = pd.to_numeric(df['revenue'], errors='coerce')
            
            print(f"    ✅ {len(df)}개 노출 데이터")
            return df
            
        except Exception as e:
            print(f"    ❌ 에러: {str(e)}")
            return None
    
    def fetch_aggregated_revenue(self, date):
        """Revenue Reporting API 호출"""
        print(f"  📊 Aggregated Revenue 데이터 조회")
        
        url = "https://r.applovin.com/maxReport"
        params = {
            'api_key': self.api_key,
            'start': date,
            'end': date,
            'columns': 'day,application,package_name,platform,country,device_type,'
                      'ad_format,max_ad_unit_id,max_placement,network,network_placement,'
                      'impressions,estimated_revenue,ecpm,requests',
            'format': 'csv',
            'not_zero': 1
        }
        
        try:
            response = requests.get(url, params=params, timeout=60)
            response.raise_for_status()
            
            df = pd.read_csv(StringIO(response.text))
            if len(df) == 0:
                print(f"    ⚠️ 데이터 없음")
                return None
            
            df.rename(columns={'day': 'report_date'}, inplace=True)
            df['report_hour'] = None
            df['loaded_at'] = datetime.utcnow()
            
            df['report_date'] = pd.to_datetime(df['report_date']).dt.date
            df['impressions'] = df['impressions'].astype(int)
            df['estimated_revenue'] = df['estimated_revenue'].astype(float)
            df['ecpm'] = df['ecpm'].astype(float)
            
            print(f"    ✅ {len(df)}개 집계 레코드")
            return df
            
        except Exception as e:
            print(f"    ❌ 에러: {str(e)}")
            return None
    
    def load_to_bigquery(self, df, table_name):
        """DataFrame을 BigQuery에 적재"""
        if df is None or len(df) == 0:
            return
        
        table_ref = f"{self.project_id}.{self.dataset_id}.{table_name}"
        job_config = bigquery.LoadJobConfig(write_disposition="WRITE_APPEND")
        
        try:
            job = self.bq_client.load_table_from_dataframe(df, table_ref, job_config=job_config)
            job.result()
            print(f"    💾 BigQuery 적재 완료: {len(df)}개 → {table_name}")
        except Exception as e:
            print(f"    ❌ BigQuery 적재 실패: {str(e)}")
    
    def collect_daily_data(self, date=None, apps=None):
        """전체 데이터 수집 파이프라인"""
        if date is None:
            date = (datetime.utcnow() - timedelta(days=1)).strftime('%Y-%m-%d')
        
        print(f"\n{'='*50}")
        print(f"📅 {date} 데이터 수집 시작")
        print(f"{'='*50}")
        
        if apps is None:
            apps = [
                {'platform': 'android', 'package': os.environ.get('APP_PACKAGE_ANDROID')},
                {'platform': 'ios', 'package': os.environ.get('APP_PACKAGE_IOS')}
            ]
            apps = [app for app in apps if app['package']]
        
        # User-Level 데이터
        print(f"\n1️⃣ User-Level Ad Revenue 수집")
        all_user_data = []
        for app in apps:
            df = self.fetch_user_level_data(date=date, platform=app['platform'], application=app['package'])
            if df is not None:
                all_user_data.append(df)
        
        if all_user_data:
            combined_df = pd.concat(all_user_data, ignore_index=True)
            self.load_to_bigquery(combined_df, 'raw_impressions')
        else:
            print("  ⚠️ User-Level 데이터 없음")
        
        # Aggregated Revenue
        print(f"\n2️⃣ Aggregated Revenue 수집")
        df_agg = self.fetch_aggregated_revenue(date)
        self.load_to_bigquery(df_agg, 'aggregated_revenue')
        
        print(f"\n{'='*50}")
        print(f"✅ {date} 데이터 수집 완료!")
        print(f"{'='*50}\n")

# Cloud Run 엔트리포인트
@app.route('/', methods=['GET', 'POST'])
def run_collection():
    """Cloud Scheduler에서 호출"""
    try:
        print("📥 데이터 수집 시작")
        collector = AxonDataCollector()
        collector.collect_daily_data()
        return jsonify({'status': 'success', 'time': datetime.utcnow().isoformat()}), 200
    except Exception as e:
        print(f"❌ 에러: {str(e)}")
        return jsonify({'status': 'error', 'message': str(e)}), 500

if __name__ == "__main__":
    app.run(host='0.0.0.0', port=int(os.environ.get('PORT', 8080)))