from flask import Flask, jsonify
import os
import json
from datetime import datetime, timedelta
import pandas as pd
import requests
from io import StringIO
from google.cloud import bigquery

app = Flask(__name__)

class AxonDataCollector:
    def __init__(self):
        """환경변수 로드"""
        self.api_key = os.environ['AXON_API_KEY']
        self.project_id = os.environ['GCP_PROJECT_ID']
        self.dataset_id = os.environ['BQ_DATASET_ID']
        self.bq_client = bigquery.Client(project=self.project_id)
        
        # JSON 형식으로 앱 목록 로드
        apps_config = os.environ.get('APPS_CONFIG', '[]')
        self.apps = json.loads(apps_config)
        print(f"📱 등록된 앱: {len(self.apps)}개")
    
    def check_data_exists(self, table_name, date):
        """특정 날짜 데이터 존재 여부 확인"""
        query = f"""
        SELECT COUNT(*) as cnt
        FROM `{self.project_id}.{self.dataset_id}.{table_name}`
        WHERE report_date = '{date}'
        """
        try:
            result = self.bq_client.query(query).result()
            count = list(result)[0].cnt
            return count > 0
        except Exception as e:
            print(f"    ⚠️ 테이블 확인 실패 ({table_name}): {str(e)}")
            return False
    
    def delete_date_data(self, table_name, date):
        """특정 날짜 데이터 삭제 (업데이트 전)"""
        query = f"""
        DELETE FROM `{self.project_id}.{self.dataset_id}.{table_name}`
        WHERE report_date = '{date}'
        """
        try:
            job = self.bq_client.query(query)
            job.result()
            print(f"    🗑️ 기존 데이터 삭제: {date}")
        except Exception as e:
            print(f"    ❌ 삭제 실패: {str(e)}")
    
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
        
        # requests 제거 + max_placement도 제거 (간소화)
        params = {
            'api_key': self.api_key,
            'start': date,
            'end': date,
            'columns': 'day,application,package_name,platform,country,device_type,'
                    'ad_format,impressions,estimated_revenue,ecpm',
            'format': 'csv',
            'not_zero': 1
        }
        
        try:
            response = requests.get(url, params=params, timeout=60)
            response.raise_for_status()
            
            df = pd.read_csv(StringIO(response.text))
            
            # 디버깅
            print(f"    📋 API 응답 컬럼: {df.columns.tolist()}")
            
            if len(df) == 0:
                print(f"    ⚠️ 데이터 없음")
                return None
            
            # 컬럼 rename
            df.rename(columns={'day': 'report_date'}, inplace=True)
            df['report_hour'] = None
            df['loaded_at'] = datetime.utcnow()
            
            # 타입 변환
            df['report_date'] = pd.to_datetime(df['report_date']).dt.date
            df['impressions'] = pd.to_numeric(df['impressions'], errors='coerce').fillna(0).astype(int)
            df['estimated_revenue'] = pd.to_numeric(df['estimated_revenue'], errors='coerce').fillna(0).astype(float)
            df['ecpm'] = pd.to_numeric(df['ecpm'], errors='coerce').fillna(0).astype(float)
            
            print(f"    ✅ {len(df)}개 집계 레코드")
            return df
            
        except requests.exceptions.HTTPError as e:
            print(f"    ❌ HTTP 에러: {e}")
            if hasattr(e.response, 'text'):
                print(f"    📝 응답: {e.response.text[:200]}")
            return None
        except Exception as e:
            print(f"    ❌ 에러: {str(e)}")
            return None


    def load_to_bigquery(self, df, table_name, date, force_update=False):
        """
        DataFrame을 BigQuery에 적재
        
        Args:
            df: 적재할 데이터
            table_name: 테이블명
            date: 날짜 (중복 체크용)
            force_update: True면 기존 데이터 삭제 후 재적재
        """
        if df is None or len(df) == 0:
            return
        
        table_ref = f"{self.project_id}.{self.dataset_id}.{table_name}"
        
        # 데이터 존재 여부 확인
        exists = self.check_data_exists(table_name, date)
        
        if exists and not force_update:
            print(f"    ⏭️ 이미 데이터 존재, 스킵: {date} → {table_name}")
            return
        
        if exists and force_update:
            print(f"    🔄 데이터 업데이트 모드: {date}")
            self.delete_date_data(table_name, date)
        
        job_config = bigquery.LoadJobConfig(write_disposition="WRITE_APPEND")
        
        try:
            job = self.bq_client.load_table_from_dataframe(df, table_ref, job_config=job_config)
            job.result()
            print(f"    💾 BigQuery 적재 완료: {len(df)}개 → {table_name}")
        except Exception as e:
            print(f"    ❌ BigQuery 적재 실패: {str(e)}")
    
    def collect_daily_data(self, date=None, apps=None, force_update=False):
        """
        전체 데이터 수집 파이프라인
        
        Args:
            date: 수집 날짜 (기본값: 어제)
            apps: 앱 목록 (기본값: 환경변수)
            force_update: 기존 데이터 있어도 업데이트
        """
        if date is None:
            date = (datetime.utcnow() - timedelta(days=1)).strftime('%Y-%m-%d')
        
        print(f"\n{'='*50}")
        print(f"📅 {date} 데이터 수집 시작")
        if force_update:
            print(f"🔄 업데이트 모드: 기존 데이터 덮어쓰기")
        print(f"{'='*50}")
        
        # 환경변수에서 앱 목록 가져오기
        if apps is None:
            apps = self.apps
        
        if not apps:
            print("⚠️ 등록된 앱이 없습니다. APPS_CONFIG 환경변수를 확인하세요.")
            return
        
        # User-Level 데이터
        print(f"\n1️⃣ User-Level Ad Revenue 수집")
        all_user_data = []
        for app in apps:
            df = self.fetch_user_level_data(
                date=date,
                platform=app['platform'],
                application=app['package']
            )
            if df is not None:
                all_user_data.append(df)
        
        if all_user_data:
            combined_df = pd.concat(all_user_data, ignore_index=True)
            self.load_to_bigquery(combined_df, 'raw_impressions', date, force_update)
        else:
            print("  ⚠️ User-Level 데이터 없음")
        
        # Aggregated Revenue
        print(f"\n2️⃣ Aggregated Revenue 수집")
        df_agg = self.fetch_aggregated_revenue(date)
        self.load_to_bigquery(df_agg, 'aggregated_revenue', date, force_update)
        
        print(f"\n{'='*50}")
        print(f"✅ {date} 데이터 수집 완료!")
        print(f"{'='*50}\n")
    
    def backfill_data(self, days=45):
        """
        과거 데이터 초기 적재 (45일)
        
        Args:
            days: 과거 며칠치 데이터 수집
        """
        print(f"\n{'🔥'*25}")
        print(f"🔥 초기 데이터 적재: 과거 {days}일")
        print(f"{'🔥'*25}\n")
        
        today = datetime.utcnow()
        
        for i in range(days, 0, -1):
            target_date = (today - timedelta(days=i)).strftime('%Y-%m-%d')
            self.collect_daily_data(date=target_date, force_update=False)
        
        print(f"\n{'🎉'*25}")
        print(f"🎉 초기 적재 완료: {days}일치 데이터")
        print(f"{'🎉'*25}\n")

# Cloud Run 엔트리포인트
@app.route('/', methods=['GET', 'POST'])
def run_collection():
    """Cloud Scheduler에서 호출 (일일 업데이트)"""
    try:
        print("📥 일일 데이터 수집 시작")
        collector = AxonDataCollector()
        
        # 어제 데이터만 수집 (중복 체크 포함)
        collector.collect_daily_data(force_update=False)
        
        return jsonify({'status': 'success', 'time': datetime.utcnow().isoformat()}), 200
    except Exception as e:
        print(f"❌ 에러: {str(e)}")
        return jsonify({'status': 'error', 'message': str(e)}), 500

@app.route('/backfill', methods=['POST'])
def backfill():
    """초기 45일 데이터 적재 (수동 호출용)"""
    try:
        print("🔥 초기 데이터 적재 시작")
        collector = AxonDataCollector()
        collector.backfill_data(days=45)
        return jsonify({'status': 'success', 'message': '45일 데이터 적재 완료'}), 200
    except Exception as e:
        print(f"❌ 에러: {str(e)}")
        return jsonify({'status': 'error', 'message': str(e)}), 500

@app.route('/force-update', methods=['POST'])
def force_update():
    """강제 업데이트 (어제 데이터 재수집)"""
    try:
        print("🔄 강제 업데이트 시작")
        collector = AxonDataCollector()
        collector.collect_daily_data(force_update=True)
        return jsonify({'status': 'success', 'message': '데이터 강제 업데이트 완료'}), 200
    except Exception as e:
        print(f"❌ 에러: {str(e)}")
        return jsonify({'status': 'error', 'message': str(e)}), 500

if __name__ == "__main__":
    app.run(host='0.0.0.0', port=int(os.environ.get('PORT', 8080)))