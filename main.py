from flask import Flask, jsonify
import os
import json
from datetime import datetime, timedelta
import pandas as pd
import requests
from io import StringIO
from google.cloud import bigquery
import time

app = Flask(__name__)

class AxonDataCollector:
    def __init__(self):
        """환경변수 로드 및 초기화"""
        # 필수 환경변수 검증
        required_env_vars = ['AXON_API_KEY', 'GCP_PROJECT_ID', 'BQ_DATASET_ID']
        missing_vars = [var for var in required_env_vars if not os.environ.get(var)]

        if missing_vars:
            raise ValueError(f"❌ 필수 환경변수 누락: {', '.join(missing_vars)}")

        self.api_key = os.environ['AXON_API_KEY']
        self.project_id = os.environ['GCP_PROJECT_ID']
        self.dataset_id = os.environ['BQ_DATASET_ID']

        # BigQuery 클라이언트 초기화
        try:
            self.bq_client = bigquery.Client(project=self.project_id)
        except Exception as e:
            raise ValueError(f"❌ BigQuery 클라이언트 초기화 실패: {str(e)}")

        # JSON 형식으로 앱 목록 로드
        apps_config = os.environ.get('APPS_CONFIG', '[]')
        try:
            self.apps = json.loads(apps_config)
            if not isinstance(self.apps, list):
                raise ValueError("APPS_CONFIG는 리스트 형식이어야 합니다")

            # 앱 설정 검증
            for idx, app in enumerate(self.apps):
                if 'platform' not in app or 'package' not in app:
                    raise ValueError(f"앱 {idx}: 'platform'과 'package' 필드가 필요합니다")
                if app['platform'] not in ['android', 'ios', 'fireos']:
                    raise ValueError(f"앱 {idx}: platform은 android, ios, fireos 중 하나여야 합니다")

            print(f"📱 등록된 앱: {len(self.apps)}개")
        except json.JSONDecodeError as e:
            raise ValueError(f"❌ APPS_CONFIG JSON 파싱 실패: {str(e)}")
    
    def check_data_exists(self, table_name, date, application=None, platform=None):
        """
        특정 날짜 데이터 존재 여부 확인

        Args:
            table_name: 테이블명
            date: 날짜
            application: 앱 패키지명 (user_level_ad_revenue만 해당)
            platform: 플랫폼 (user_level_ad_revenue만 해당)
        """
        # user_level_ad_revenue는 앱별로 확인
        if table_name == 'user_level_ad_revenue' and application and platform:
            query = f"""
            SELECT COUNT(*) as cnt
            FROM `{self.project_id}.{self.dataset_id}.{table_name}`
            WHERE report_date = '{date}'
              AND application = '{application}'
              AND platform = '{platform}'
            """
        else:
            # revenue_reporting은 날짜별로 확인
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
    
    def delete_date_data(self, table_name, date, application=None, platform=None):
        """
        특정 날짜 데이터 삭제 (업데이트 전)

        Args:
            table_name: 테이블명
            date: 날짜
            application: 앱 패키지명 (user_level_ad_revenue만 해당)
            platform: 플랫폼 (user_level_ad_revenue만 해당)
        """
        # user_level_ad_revenue는 앱별로 삭제 (같은 날짜의 다른 앱 보호)
        if table_name == 'user_level_ad_revenue' and application and platform:
            query = f"""
            DELETE FROM `{self.project_id}.{self.dataset_id}.{table_name}`
            WHERE report_date = '{date}'
              AND application = '{application}'
              AND platform = '{platform}'
            """
            print(f"    🗑️ 기존 데이터 삭제: {date} / {application} ({platform})")
        else:
            # revenue_reporting은 날짜별로 삭제
            query = f"""
            DELETE FROM `{self.project_id}.{self.dataset_id}.{table_name}`
            WHERE report_date = '{date}'
            """
            print(f"    🗑️ 기존 데이터 삭제: {date}")

        try:
            job = self.bq_client.query(query)
            job.result()
        except Exception as e:
            print(f"    ❌ 삭제 실패: {str(e)}")
    
    def fetch_user_level_data(self, date, platform, application, aggregated=False):
        """
        User-Level Ad Revenue API 호출

        Args:
            date: 조회 날짜 (YYYY-MM-DD)
            platform: android, ios, fireos
            application: 패키지명
            aggregated: False=노출별(추천), True=유저별 집계
        """
        data_type_str = "유저별 집계" if aggregated else "노출별"
        print(f"  📥 User-Level ({data_type_str}) 데이터 조회: {application} ({platform})")

        url = "https://r.applovin.com/max/userAdRevenueReport"
        params = {
            'api_key': self.api_key,
            'date': date,
            'platform': platform,
            'application': application,
            'aggregated': 'true' if aggregated else 'false'
        }

        try:
            response = requests.get(url, params=params, timeout=60)

            # HTTP 상태 코드별 처리
            if response.status_code == 429:
                print(f"    ⚠️ Rate limit 초과 (429) - 60초 대기 후 재시도")
                time.sleep(60)
                response = requests.get(url, params=params, timeout=60)
            elif response.status_code == 401:
                print(f"    ❌ 인증 실패 (401) - API 키 확인 필요")
                return None
            elif response.status_code == 403:
                print(f"    ❌ 접근 거부 (403) - 권한 확인 필요")
                return None
            elif response.status_code >= 500:
                print(f"    ⚠️ 서버 에러 ({response.status_code}) - 30초 대기 후 재시도")
                time.sleep(30)
                response = requests.get(url, params=params, timeout=60)

            response.raise_for_status()
            data = response.json()

            # 3가지 URL 우선순위: ad_revenue_report_url (전체) > url > fb_estimated_revenue_url
            csv_url = data.get('ad_revenue_report_url') or data.get('url') or data.get('fb_estimated_revenue_url')

            # data_source 기록
            if data.get('ad_revenue_report_url'):
                data_source = 'ad_revenue_report_url'
            elif data.get('url'):
                data_source = 'url'
            elif data.get('fb_estimated_revenue_url'):
                data_source = 'fb_estimated_revenue_url'
            else:
                print(f"    ⚠️ CSV URL 없음 (정상 - 데이터 없는 날)")
                return None

            # S3 URL은 1시간 내 만료되므로 즉시 다운로드
            csv_response = requests.get(csv_url, timeout=120)
            csv_response.raise_for_status()

            df = pd.read_csv(StringIO(csv_response.text))

            if len(df) == 0:
                print(f"    ⚠️ 데이터 없음 (정상 - 신규 앱이거나 트래픽 없음)")
                return None

            # 컬럼명 정규화 (대소문자, 공백 통일)
            df.columns = df.columns.str.strip().str.lower().str.replace(' ', '_')

            # 동적 컬럼 매핑 (API 응답이 일관성 없을 수 있음)
            column_mapping = {}
            for col in df.columns:
                if col in ['date', 'timestamp']:
                    column_mapping[col] = 'impression_timestamp'
                elif col in ['ad_unit_id', 'adunitid']:
                    column_mapping[col] = 'ad_unit_id'
                elif col in ['ad_unit_name', 'adunitname']:
                    column_mapping[col] = 'ad_unit_name'
                elif col in ['ad_format', 'adformat']:
                    column_mapping[col] = 'ad_format'
                elif col in ['ad_placement', 'adplacement']:
                    column_mapping[col] = 'ad_placement'
                elif col in ['device_type', 'devicetype']:
                    column_mapping[col] = 'device_type'
                elif col in ['user_id', 'userid']:
                    column_mapping[col] = 'user_id'
                elif col in ['custom_data', 'customdata']:
                    column_mapping[col] = 'custom_data'

            df.rename(columns=column_mapping, inplace=True)

            # 필수 컬럼 추가
            df['report_date'] = pd.to_datetime(date).date()
            df['application'] = application
            df['package_name'] = application
            df['platform'] = platform
            df['loaded_at'] = datetime.utcnow()
            df['data_source'] = data_source

            # data_type 구분 (impressions 컬럼 유무로 판단)
            if 'impressions' in df.columns:
                df['data_type'] = 'user_aggregated'
            else:
                df['data_type'] = 'impression'

            # 타입 변환
            if 'impression_timestamp' in df.columns:
                df['impression_timestamp'] = pd.to_datetime(df['impression_timestamp'], errors='coerce')

            if 'revenue' in df.columns:
                df['revenue'] = pd.to_numeric(df['revenue'], errors='coerce')

            if 'impressions' in df.columns:
                df['impressions'] = pd.to_numeric(df['impressions'], errors='coerce').fillna(0).astype(int)

            # NULL 처리 (IDFA, User ID 등은 NULL 가능)
            # pandas의 NaN을 None으로 변환 (BigQuery는 None을 NULL로 처리)
            df = df.where(pd.notnull(df), None)

            record_count = len(df)
            print(f"    ✅ {record_count}개 레코드 ({data_source})")
            return df

        except Exception as e:
            print(f"    ❌ 에러: {str(e)}")
            import traceback
            print(f"    📝 상세: {traceback.format_exc()}")
            return None


    def fetch_revenue_reporting_basic(self, date):
        """Revenue Reporting API 호출 - Basic (requests 포함)"""
        print(f"  📊 Revenue Reporting (Basic) 데이터 조회")

        url = "https://r.applovin.com/maxReport"
        params = {
            'api_key': self.api_key,
            'start': date,
            'end': date,
            'columns': 'day,application,package_name,store_id,platform,country,device_type,'
                    'ad_format,has_idfa,impressions,estimated_revenue,ecpm,requests',
            'format': 'csv',
            'not_zero': 1
        }

        try:
            response = requests.get(url, params=params, timeout=60)

            # HTTP 상태 코드별 처리
            if response.status_code == 429:
                print(f"    ⚠️ Rate limit 초과 (429) - 60초 대기 후 재시도")
                time.sleep(60)
                response = requests.get(url, params=params, timeout=60)
            elif response.status_code == 401:
                print(f"    ❌ 인증 실패 (401) - API 키 확인 필요")
                return None
            elif response.status_code == 403:
                print(f"    ❌ 접근 거부 (403) - 권한 확인 필요")
                return None
            elif response.status_code >= 500:
                print(f"    ⚠️ 서버 에러 ({response.status_code}) - 30초 대기 후 재시도")
                time.sleep(30)
                response = requests.get(url, params=params, timeout=60)

            response.raise_for_status()

            df = pd.read_csv(StringIO(response.text))

            if len(df) == 0:
                print(f"    ⚠️ 데이터 없음")
                return None

            # 컬럼명 소문자 변환
            df.columns = df.columns.str.lower()

            # 컬럼 rename
            df.rename(columns={'day': 'report_date'}, inplace=True)

            # 필수 컬럼 추가
            df['query_type'] = 'basic'
            df['loaded_at'] = datetime.utcnow()

            # 안전한 타입 변환
            if 'report_date' in df.columns:
                df['report_date'] = pd.to_datetime(df['report_date']).dt.date
            else:
                print(f"    ❌ 'report_date' 컬럼 없음!")
                return None

            # 숫자형 컬럼 변환
            if 'impressions' in df.columns:
                df['impressions'] = pd.to_numeric(df['impressions'], errors='coerce').fillna(0).astype(int)
            if 'estimated_revenue' in df.columns:
                df['estimated_revenue'] = pd.to_numeric(df['estimated_revenue'], errors='coerce').fillna(0).astype(float)
            if 'ecpm' in df.columns:
                df['ecpm'] = pd.to_numeric(df['ecpm'], errors='coerce').fillna(0).astype(float)
            if 'requests' in df.columns:
                df['requests'] = pd.to_numeric(df['requests'], errors='coerce').fillna(0).astype(int)

            # boolean 컬럼 변환
            if 'has_idfa' in df.columns:
                df['has_idfa'] = df['has_idfa'].astype(str).str.lower().isin(['true', '1', 'yes'])
            if 'max_ad_unit_test' in df.columns:
                df['max_ad_unit_test'] = df['max_ad_unit_test'].astype(str).str.lower().isin(['true', '1', 'yes'])

            print(f"    ✅ {len(df)}개 Basic 레코드")
            return df

        except Exception as e:
            print(f"    ❌ 에러: {str(e)}")
            import traceback
            print(f"    📝 상세: {traceback.format_exc()}")
            return None

    def fetch_revenue_reporting_network(self, date):
        """Revenue Reporting API 호출 - Network Detail (attempts, responses, fill_rate 포함)"""
        print(f"  📊 Revenue Reporting (Network Detail) 데이터 조회")

        url = "https://r.applovin.com/maxReport"
        params = {
            'api_key': self.api_key,
            'start': date,
            'end': date,
            'columns': 'day,application,package_name,platform,country,device_type,'
                    'ad_format,network,network_placement,custom_network_name,has_idfa,'
                    'impressions,estimated_revenue,ecpm,attempts,responses,fill_rate',
            'format': 'csv',
            'not_zero': 1
        }

        try:
            response = requests.get(url, params=params, timeout=60)

            # HTTP 상태 코드별 처리
            if response.status_code == 429:
                print(f"    ⚠️ Rate limit 초과 (429) - 60초 대기 후 재시도")
                time.sleep(60)
                response = requests.get(url, params=params, timeout=60)
            elif response.status_code == 401:
                print(f"    ❌ 인증 실패 (401) - API 키 확인 필요")
                return None
            elif response.status_code == 403:
                print(f"    ❌ 접근 거부 (403) - 권한 확인 필요")
                return None
            elif response.status_code >= 500:
                print(f"    ⚠️ 서버 에러 ({response.status_code}) - 30초 대기 후 재시도")
                time.sleep(30)
                response = requests.get(url, params=params, timeout=60)

            response.raise_for_status()

            df = pd.read_csv(StringIO(response.text))

            if len(df) == 0:
                print(f"    ⚠️ 데이터 없음")
                return None

            # 컬럼명 소문자 변환
            df.columns = df.columns.str.lower()

            # 컬럼 rename
            df.rename(columns={'day': 'report_date'}, inplace=True)

            # 필수 컬럼 추가
            df['query_type'] = 'network_detail'
            df['loaded_at'] = datetime.utcnow()

            # 안전한 타입 변환
            if 'report_date' in df.columns:
                df['report_date'] = pd.to_datetime(df['report_date']).dt.date
            else:
                print(f"    ❌ 'report_date' 컬럼 없음!")
                return None

            # 숫자형 컬럼 변환
            if 'impressions' in df.columns:
                df['impressions'] = pd.to_numeric(df['impressions'], errors='coerce').fillna(0).astype(int)
            if 'estimated_revenue' in df.columns:
                df['estimated_revenue'] = pd.to_numeric(df['estimated_revenue'], errors='coerce').fillna(0).astype(float)
            if 'ecpm' in df.columns:
                df['ecpm'] = pd.to_numeric(df['ecpm'], errors='coerce').fillna(0).astype(float)
            if 'attempts' in df.columns:
                df['attempts'] = pd.to_numeric(df['attempts'], errors='coerce').fillna(0).astype(int)
            if 'responses' in df.columns:
                df['responses'] = pd.to_numeric(df['responses'], errors='coerce').fillna(0).astype(int)
            if 'fill_rate' in df.columns:
                df['fill_rate'] = pd.to_numeric(df['fill_rate'], errors='coerce').fillna(0).astype(float)

            # boolean 컬럼 변환
            if 'has_idfa' in df.columns:
                df['has_idfa'] = df['has_idfa'].astype(str).str.lower().isin(['true', '1', 'yes'])

            print(f"    ✅ {len(df)}개 Network Detail 레코드")
            return df

        except Exception as e:
            print(f"    ❌ 에러: {str(e)}")
            import traceback
            print(f"    📝 상세: {traceback.format_exc()}")
            return None



    def load_to_bigquery(self, df, table_name, date, force_update=False, application=None, platform=None):
        """
        DataFrame을 BigQuery에 적재

        Args:
            df: 적재할 데이터
            table_name: 테이블명
            date: 날짜 (중복 체크용)
            force_update: True면 기존 데이터 삭제 후 재적재
            application: 앱 패키지명 (user_level_ad_revenue용)
            platform: 플랫폼 (user_level_ad_revenue용)
        """
        if df is None or len(df) == 0:
            return

        table_ref = f"{self.project_id}.{self.dataset_id}.{table_name}"

        # 데이터 존재 여부 확인 (user_level_ad_revenue는 앱별로 체크)
        exists = self.check_data_exists(table_name, date, application, platform)

        if exists and not force_update:
            if table_name == 'user_level_ad_revenue' and application and platform:
                print(f"    ⏭️ 이미 데이터 존재, 스킵: {date} / {application} ({platform}) → {table_name}")
            else:
                print(f"    ⏭️ 이미 데이터 존재, 스킵: {date} → {table_name}")
            return

        if exists and force_update:
            print(f"    🔄 데이터 업데이트 모드: {date}")
            self.delete_date_data(table_name, date, application, platform)

        job_config = bigquery.LoadJobConfig(write_disposition="WRITE_APPEND")

        try:
            job = self.bq_client.load_table_from_dataframe(df, table_ref, job_config=job_config)
            job.result()
            print(f"    💾 BigQuery 적재 완료: {len(df)}개 → {table_name}")
        except Exception as e:
            print(f"    ❌ BigQuery 적재 실패: {str(e)}")
            import traceback
            print(f"    📝 상세: {traceback.format_exc()}")
    
    def collect_daily_data(self, date=None, apps=None, force_update=False):
        """
        전체 데이터 수집 파이프라인

        Args:
            date: 수집 날짜 (기본값: 어제)
            apps: 앱 목록 (기본값: 환경변수)
            force_update: 기존 데이터 있어도 업데이트

        Returns:
            dict: 수집 결과 통계
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
            return {'success': False, 'message': '앱 목록 없음'}

        # 통계 카운터
        stats = {
            'user_level_success': 0,
            'user_level_failed': 0,
            'user_level_no_data': 0,
            'revenue_basic_success': False,
            'revenue_network_success': False
        }

        # User-Level 데이터 (노출별 - aggregated=false)
        print(f"\n1️⃣ User-Level Ad Revenue 수집 (노출별)")
        for app in apps:
            try:
                df = self.fetch_user_level_data(
                    date=date,
                    platform=app['platform'],
                    application=app['package'],
                    aggregated=False  # 노출별 데이터 (추천)
                )
                if df is not None:
                    # 앱별로 개별 적재 (중복 방지를 위해)
                    self.load_to_bigquery(
                        df,
                        'user_level_ad_revenue',
                        date,
                        force_update,
                        application=app['package'],
                        platform=app['platform']
                    )
                    stats['user_level_success'] += 1
                else:
                    stats['user_level_no_data'] += 1
            except Exception as e:
                print(f"    ❌ {app['package']} ({app['platform']}) 처리 실패: {str(e)}")
                stats['user_level_failed'] += 1

        # Revenue Reporting - Basic
        print(f"\n2️⃣ Revenue Reporting 수집")
        try:
            df_basic = self.fetch_revenue_reporting_basic(date)
            if df_basic is not None:
                self.load_to_bigquery(df_basic, 'revenue_reporting', date, force_update)
                stats['revenue_basic_success'] = True
        except Exception as e:
            print(f"    ❌ Revenue Reporting (Basic) 처리 실패: {str(e)}")

        # Revenue Reporting - Network Detail
        try:
            df_network = self.fetch_revenue_reporting_network(date)
            if df_network is not None:
                self.load_to_bigquery(df_network, 'revenue_reporting', date, force_update)
                stats['revenue_network_success'] = True
        except Exception as e:
            print(f"    ❌ Revenue Reporting (Network) 처리 실패: {str(e)}")

        # 결과 출력
        print(f"\n{'='*50}")
        print(f"✅ {date} 데이터 수집 완료!")
        print(f"📊 User-Level: 성공 {stats['user_level_success']}, "
              f"데이터없음 {stats['user_level_no_data']}, "
              f"실패 {stats['user_level_failed']}")
        print(f"📊 Revenue Reporting: Basic {'✅' if stats['revenue_basic_success'] else '❌'}, "
              f"Network {'✅' if stats['revenue_network_success'] else '❌'}")
        print(f"{'='*50}\n")

        return stats
    
    def backfill_data(self, days=45):
        """
        과거 데이터 초기 적재 (45일)

        Args:
            days: 과거 며칠치 데이터 수집

        Returns:
            dict: 전체 수집 결과 통계
        """
        print(f"\n{'🔥'*25}")
        print(f"🔥 초기 데이터 적재: 과거 {days}일")
        print(f"{'🔥'*25}\n")

        today = datetime.utcnow()

        # 전체 통계
        total_stats = {
            'total_days': days,
            'success_days': 0,
            'failed_days': 0,
            'start_time': datetime.utcnow()
        }

        for i in range(days, 0, -1):
            try:
                target_date = (today - timedelta(days=i)).strftime('%Y-%m-%d')

                # 진행률 표시
                progress = ((days - i + 1) / days) * 100
                print(f"📈 진행률: {progress:.1f}% ({days - i + 1}/{days})")

                stats = self.collect_daily_data(date=target_date, force_update=False)

                # 성공 여부 판단
                if stats and (stats.get('user_level_success', 0) > 0 or
                             stats.get('revenue_basic_success', False) or
                             stats.get('revenue_network_success', False)):
                    total_stats['success_days'] += 1
                else:
                    total_stats['failed_days'] += 1

            except Exception as e:
                print(f"❌ {target_date} 처리 중 오류: {str(e)}")
                total_stats['failed_days'] += 1

        total_stats['end_time'] = datetime.utcnow()
        total_stats['duration'] = (total_stats['end_time'] - total_stats['start_time']).total_seconds()

        # 결과 요약
        print(f"\n{'🎉'*25}")
        print(f"🎉 초기 적재 완료: {days}일치 데이터")
        print(f"📊 성공: {total_stats['success_days']}일, 실패: {total_stats['failed_days']}일")
        print(f"⏱️ 소요 시간: {total_stats['duration']:.1f}초 ({total_stats['duration']/60:.1f}분)")
        print(f"{'🎉'*25}\n")

        return total_stats

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