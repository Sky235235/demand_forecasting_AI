import pandas as pd
import numpy as np
import joblib
from tqdm import tqdm
from datetime import datetime
from datetime import date, timedelta
import math

class RM_duplicate:

    def __init__(self, data):
        self.data = data

    def get_nodup(self):
        origin_dup = self.data
        origin_dup['timestamp'] = origin_dup['이용일시'].apply(lambda x : x.timestamp())
        origin_dup.loc[origin_dup['고객번호'] == '\\N', '고객번호'] = None
        # 1) 고객번호 전처리
        origin_dup['고객번호'] = origin_dup['고객번호'].fillna(0)
        origin_dup['고객번호'] = origin_dup['고객번호'].astype('int')
        origin_dup['고객번호'] = '0' + origin_dup['고객번호'].astype('str')
        origin_dup['고객번호'] = origin_dup['고객번호'].replace('00', '')

        # 2) 서비스 별 데이터프레임 구분 -> 앱호출만 중복 데이터 제거
        data_app = origin_dup[origin_dup['구분'] == '앱호출']
        data_app = data_app.reset_index().drop('index', axis=1)
        data_normal = origin_dup[origin_dup['구분'] == '일반주행']
        data_normal = data_normal.reset_index().drop('index', axis=1)
        data_call = origin_dup[origin_dup['구분'] == '예약호출']
        data_call = data_call.reset_index().drop('index', axis=1)

        # 3) 앱호출(data_app)데이터 분리 -> 앱호출 중에서 하차가 아닌 데이터만 중복 호출 제거
        data_app_pickoff = data_app[data_app['상태'] == '하차']
        data_app_pickoff = data_app_pickoff.reset_index().drop('index', axis=1)

        data_app_notyet = data_app[data_app['상태'] != '하차']
        data_app_notyet = data_app_notyet.reset_index().drop('index', axis=1)

        # 4) data_app_notyet을 고객번호, 이용일시 기준으로 sorting
        data_app_notyet = data_app_notyet.sort_values(by=['고객번호', '이용일시'])

        # 5) 컬럼 정리 배차 ID(원데이터와 join용),배차 ID, 고객번호, 이용일시, lag_이용일시
        app_notyet_lag = data_app_notyet[['배차 ID', '고객번호', 'timestamp', '이용일시']]
        app_notyet_lag = app_notyet_lag.reset_index().drop('index', axis=1)

        app_notyet_lag['lat_timestamp'] = app_notyet_lag.groupby('고객번호')['timestamp'].shift(-1)
        app_notyet_lag['tmp'] = app_notyet_lag['lat_timestamp'] - app_notyet_lag['timestamp']
        app_notyet_lag['difftime'] = app_notyet_lag['tmp'].apply(lambda x: x / 60)

        #6) 중복호출 판별 함수
        def is_duplicate(time):
            dup_yes = 0
            if time <= 5:
                dup_yes = 1
            else:
                dup_yes = 0
            return dup_yes

        app_notyet_lag['dup_yes'] = app_notyet_lag['difftime'].apply(lambda x: is_duplicate(x))

        # 7) 앱호출 미하차 데이터 병합
        merge_data = app_notyet_lag[['배차 ID', 'dup_yes']]
        data_app_notyet = pd.merge(data_app_notyet, merge_data, on='배차 ID', how='left')
        data_app_notyet = data_app_notyet[data_app_notyet['dup_yes'] != 1]
        data_app_notyet = data_app_notyet.reset_index().drop(['index', 'dup_yes'], axis=1)

        # 8) 앱호출 미하차 데이터와 하차 데이터 concatenate
        data_app = pd.concat([data_app_notyet, data_app_pickoff])
        data_app = data_app.sort_values(by='배차 ID')
        data_app = data_app.reset_index().drop('index', axis=1)

        ## 2. 최종 데이터 중복호출 제거
        data_app = data_app.sort_values(by=['고객번호', '이용일시'])
        data_app['lag_timestamp'] = data_app.groupby('고객번호')['timestamp'].shift(-1)
        data_app['tmp'] = data_app['lag_timestamp'] - data_app['timestamp']
        data_app['difftime'] = data_app['tmp'].apply(lambda x: x / 60)
        data_app['dup_yes'] = data_app['difftime'].apply(lambda x: is_duplicate(x))
        data_app = data_app.reset_index().drop('index', axis=1)

        # 하차인 데이터 제외 dup_yes == 1 이 더라도 하차상태는 제외하기 위함
        app_pickoff = data_app[data_app['상태'] == '하차']
        app_nodispatch = data_app[data_app['상태'] != '하차']

        # 하차가 아닌 데이터에 대해 dup_yes ==1을 드랍
        app_nodispatch = app_nodispatch[app_nodispatch['dup_yes'] != 1]
        # 데이터 다시 병합
        data_app = pd.concat([app_pickoff, app_nodispatch])
        data_app = data_app.sort_values(by='이용일시')
        data_app = data_app.reset_index().drop('index', axis=1)

        origin_nodup = pd.concat([data_app, data_normal, data_call])
        origin_nodup = origin_nodup.sort_values(by='이용일시')
        origin_nodup = origin_nodup.reset_index().drop('index', axis=1)
        return origin_nodup


