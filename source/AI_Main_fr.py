import pandas as pd
from datetime import datetime, date, timedelta
from tqdm import tqdm
from SQL_Live import Service_LIVE_DB
from SQL_Cum import Service_OLD_DB
from Module_Rm_duplicate import RM_duplicate
import math
import warnings
warnings.filterwarnings(action='ignore')

##### 데이터 전처리 종합 #####
# 1. 중복 호출 제거 전 서비스 데이터 불러오기
print('Load Service Data')
obj = Service_LIVE_DB()
service_dup = obj.service_data()

del obj
print(service_dup.head())
print('Load Finish service Data')
# 2. 중복 호출 제거
print('Rm_duplicated_data')
nodup = RM_duplicate(service_dup)
service_nodup = nodup.get_nodup()

print('Total number of call:',len(service_dup))
print('number of duplicated call:',len(service_dup) - len(service_nodup))
print('After remove duplicated call:', len(service_nodup))

# 3.그룹화 하기
print('count per 15min')
## 1) 15분 단위 그룹화 후 ymdh로 카운팅 : 실제수요
service_nodup['ymdh']=service_nodup['이용일시'].dt.floor(freq='15min')
demand_df = service_nodup.groupby('ymdh')[['이용일시']].count().reset_index().rename(columns = {'이용일시':'y'})
## 1-1) 15분 이후 빈 데이터 만들기
now_time = datetime.now()
new_ymdh = pd.Timestamp(now_time).floor(freq='15min')
new_y = 0
new_data = {'ymdh' : new_ymdh , 'y' : new_y}
new_df = pd.DataFrame(new_data, index=['0'])
print(new_df)
demand_df = demand_df.append(new_df)
demand_df = demand_df.reset_index().drop('index',axis=1)

## 2) 카운팅된 데이터에 파생 날짜 변수 생성
demand_df['DATE'] = demand_df['ymdh'].dt.date
demand_df['WDAY'] = demand_df['ymdh'].dt.weekday
demand_df['HOUR'] = demand_df['ymdh'].dt.hour
demand_df['DATE'] = pd.to_datetime(demand_df['DATE'])
demand_df['WDAY']=demand_df['WDAY'].replace([0, 1, 2, 3, 4, 5, 6],['월', '화', '수', '목','금', '토', '일'])

# 4. 날씨 정보 결합
print('service and weather merge')
## 1) 날씨 데이터 불러오기 : 이후 예보데이터로 교체
weather = pd.read_excel('/home/elap/genieus_ai/DB_data/weather_forecasting.xlsx', engine='openpyxl')

## 2) 서비스 날씨 데이터 결합
weather['DATE'] = pd.to_datetime(weather['DATE'])
demand_df = pd.merge(demand_df, weather, on=['DATE','HOUR'], how='left')

# 5. 사회적 거리두기 결합
print('service and corona data merge')
corona = pd.read_csv('/home/elap/genieus_ai/DB_data/DB02-2_사회적거리두기.csv', encoding='cp949')
corona['date'] = pd.to_datetime(corona['date'])
corona = corona.rename(columns = {'date':'DATE'})
demand_df = pd.merge(demand_df, corona, on='DATE', how='left')

# 6. 휴일정보 결합
print('service and holiday merge')
holiday = pd.read_excel('/home/elap/genieus_ai/DB_data/holiday_2022.xlsx', engine='openpyxl')
holiday = holiday.rename(columns={"date" : "DATE"})
demand_df = pd.merge(demand_df ,holiday, on='DATE', how='left')

# 7. 데이터 전처리
print('data processing')
demand_df['name_holiday'] =demand_df['name_holiday'].fillna('일반일')
demand_df['type_holiday'] =demand_df['type_holiday'].fillna('일반일')
demand_df['holi_ind1'] = demand_df['holi_ind1'].fillna(0)
demand_df['holi_ind2'] = demand_df['holi_ind2'].fillna(0)
demand_df['holiday_yn'] = demand_df['holiday_yn'].fillna(0)
demand_df['longHoliday'] = demand_df['longHoliday'].fillna(0)
demand_df['timegrp'] = demand_df['ymdh'].dt.time
demand_df = demand_df.drop('weekdays', axis=1)

#timegrp 문자열 변경
demand_df['timegrp']=demand_df['timegrp'].astype('str')

# 8. 최종 컬럼 선택
print('select last columns')
sel_cols= ['DATE','ymdh','y','WDAY','HOUR','name_holiday','type_holiday','holiday_yn','longHoliday','holi_ind1','holi_ind2',
           '사회적거리두기','모임인원','영업종료시간','평균기온','평균강수량','timegrp']
demand_df=demand_df[sel_cols]

##### 기대효과 계산 #####
# 1. 기대효과 컬럼 생성
print('create effect columns')
new_col = ['effect_WDAYxHOUR','del_effect_WDAYxHOUR','effect_COVID1','del_effect_COVID1','effect_COVID2','del_effect_COVID2',
           'effect_HOLI','del_effect_HOLI','effect_WEATHER1','del_effect_WEATHER1','effect_WEATHER2','del_effect_WEATHER2','effect_PAST','effect_PAST2']
demand_df[new_col] = None
demand_df['DATE'] = pd.to_datetime(demand_df['DATE'], format='%Y-%m-%d')
print(demand_df.head())

# 2. 과거 적재된 최종 데이터 불러오기
past_df = pd.read_csv('/home/elap/genieus_ai/DB_data/2021_2022_final_DB_fc.csv',encoding='utf-8-sig',index_col=0)
print(past_df.columns)
past_df['DATE'] = pd.to_datetime(past_df['DATE'], format= "%Y-%m-%d")
past_df['ymdh'] = pd.to_datetime(past_df['ymdh'])
# 마지막 행의 데이터 제거
past_df = past_df.iloc[:-1, :]
print(past_df.tail())

# 3. 기대 효과 계산
# 1) 데이터 복사
now_df = demand_df.copy()
pre_df = past_df.copy()
pre_df = pre_df.reset_index().drop('index', axis=1)

# 2) 영업종료시간 전처리 (문자열 제거)
# 영업제한시간 시간만 추출
import re
def get_close_time(close):
    time = re.sub(r'[^0-9]', '', close)
    return time

now_df['영업종료시간'] = now_df['영업종료시간'].apply(lambda x: get_close_time(x))
now_df['영업종료시간'] = now_df['영업종료시간'].replace('24', '00')
pre_df['영업종료시간'] = pre_df['영업종료시간'].apply(lambda x: get_close_time(x))
pre_df['영업종료시간'] = pre_df['영업종료시간'].replace('24', '00')

# pre_df의 timegrp는 문자열 앞에 0이 없기 때문에 0 추가
def get_timegrp(timegrp):
    if len(timegrp) == 7:
        answer = timegrp.zfill(8)
        return answer
    else:
        return timegrp
pre_df['timegrp'] = pre_df['timegrp'].apply(lambda x: get_timegrp(x))

#) 3) 현재 데이터프레임 길이 만큼 기대효과 계산
for i in tqdm(range(len(now_df))):
    ## 변수 세팅
    wday = now_df['WDAY'][i]
    hour = now_df['HOUR'][i]
    timegrp = now_df['timegrp'][i]
    close_time = int(now_df['영업종료시간'][i])
    ymdh = now_df['ymdh'][i]

    # 1.effect_WDAY x HOUR
    ## 1) 영업제한시간과 현재 시간이 같을경우
    if close_time == hour:
        if hour == 0:
            effect_WDAYxHOUR = pre_df[(pre_df['WDAY'] == wday) &  (pre_df['HOUR'] == 23) & (pre_df['holiday_yn']==0)].iloc[-4:,:]['y'].mean()
        else:
            effect_WDAYxHOUR = pre_df[(pre_df['WDAY'] == wday) &  (pre_df['HOUR'] == hour-1) & (pre_df['holiday_yn']==0)].iloc[-4:,:]['y'].mean()
    else:
        effect_WDAYxHOUR = pre_df[(pre_df['WDAY'] == wday) &  (pre_df['timegrp'] == timegrp) & (pre_df['holiday_yn']==0)].iloc[-4:,:]['y'].mean()

    # 2. del_effect_WDAY x HOUR
    y = now_df['y'][i]
    del_effect_WDAYxHOUR = y - effect_WDAYxHOUR

    # 3. COVID1_effect
    social_distancing = now_df['사회적거리두기'][i]
    social_close = now_df['영업종료시간'][i]
    if close_time == hour:
        start = ymdh - timedelta(hours=1)
        end = ymdh + timedelta(minutes= 30)
        time_index = pd.date_range(start,end, freq='15T')
        sel_tgrp_list = [str(time.time()) for time in time_index]
        effect_COVID1= pre_df[(pre_df['WDAY']==wday) &
            (pre_df['사회적거리두기'] == social_distancing) &
            (pre_df['영업종료시간'] == social_close )&
            (pre_df['holiday_yn']==0) &
            (pre_df['timegrp'].isin(sel_tgrp_list))].iloc[-5:,:]['del_effect_WDAYxHOUR'].mean()
    else:
        effect_COVID1 = 0

    # 4. del_effect_COVID1
    del_effect_COVID1 = del_effect_WDAYxHOUR - effect_COVID1

    # 5. effect_COVID2
    social_distancing = now_df['사회적거리두기'][i]
    social_close = now_df['영업종료시간'][i]
    if close_time == hour:
        start = ymdh - timedelta(hours=1)
        end = ymdh + timedelta(minutes= 30)
        time_index = pd.date_range(start,end, freq='15T')
        sel_tgrp_list = [str(time.time()) for time in time_index]
        effect_COVID2= pre_df[(pre_df['WDAY']==wday) &
            (pre_df['사회적거리두기'] == social_distancing) &
            (pre_df['영업종료시간'] == social_close )&
            (pre_df['holiday_yn']==0) &
            (pre_df['timegrp'].isin(sel_tgrp_list))].iloc[-5:,:]['y'].mean()
    else:
        effect_COVID2 = 0

    # 6. del_effect_COVID2
    del_effect_COVID2 = y - effect_COVID2

    # 7. effect_HOLI
    holi_ind1 = now_df['holi_ind1'][i]
    holi_ind2 = now_df['holi_ind2'][i]
    if holi_ind1 == 1:
        effect_HOLI = pre_df[(pre_df['timegrp'] == timegrp) & (pre_df['holi_ind1']==1)].iloc[-3:,:]['del_effect_COVID1'].mean()
    elif holi_ind2 == 1:
        effect_HOLI = pre_df[(pre_df['timegrp'] == timegrp) & (pre_df['holi_ind2']==1)].iloc[-3:,:]['del_effect_COVID1'].mean()
    else:
        effect_HOLI = 0

    # 8. del_effect_HOLI
    del_effect_HOLI = del_effect_COVID1 - effect_HOLI

    # 9. effect_WEATHER1
    temp = now_df['평균기온'][i]
    if temp >= 30:
        effect_WEATHER1 = pre_df[(pre_df['timegrp'] == timegrp) & (pre_df['평균기온']>=30)].iloc[-5:,:]['del_effect_HOLI'].mean()
    else:
        effect_WEATHER1 = 0

    # 10. del_effect_WEATHER1
    del_effect_WEATHER1 = del_effect_HOLI - effect_WEATHER1

    # 11. effect_WEATHER2
    rain = now_df['평균강수량'][i]
    if rain >= 10:
        effect_WEATHER2 = pre_df[(pre_df['timegrp'] == timegrp) & (pre_df['평균강수량']>=10)].iloc[-5:,:]['del_effect_WEATHER1'].mean()
    else:
        effect_WEATHER2 = 0

    # 12. del_effect_WEATHER2
    del_effect_WEATHER2 = del_effect_WEATHER1 - effect_WEATHER2

    # # 13. effect_PAST
    # pre_ymdh = ymdh - timedelta(minutes = 15)
    # print(pre_ymdh)
    # effect_PAST = pre_df[pre_df['ymdh'] == pre_ymdh]['y'].values[0]
    #
    # # 14. effect_PAST2
    # pre_day_ymdh = ymdh - timedelta(days = 1)
    # effect_PAST2= pre_df[pre_df['ymdh'] == pre_day_ymdh]['y'].values[0]

    # 13. effect_PAST
    pre_ymdh = ymdh - timedelta(minutes = 15)

    if pre_df[pre_df['ymdh'] == pre_ymdh]['y'].size > 0:
        effect_PAST = pre_df[pre_df['ymdh'] == pre_ymdh]['y'].values[0]

    else:
        effect_PAST = 0

    # 14. effect_PAST2
    pre_day_ymdh = ymdh - timedelta(days = 1)

    if pre_df[pre_df['ymdh'] == pre_day_ymdh]['y'].size > 0:
        effect_PAST2 = pre_df[pre_df['ymdh'] == pre_day_ymdh]['y'].values[0]

    else:
        effect_PAST2 = 0

    # 최종 생성된 변수 original df(demand_df)에 추가
    demand_df.loc[demand_df['ymdh']==ymdh, 'effect_WDAYxHOUR'] = effect_WDAYxHOUR
    demand_df.loc[demand_df['ymdh']==ymdh, 'del_effect_WDAYxHOUR'] = del_effect_WDAYxHOUR
    demand_df.loc[demand_df['ymdh']==ymdh, 'effect_COVID1'] = effect_COVID1
    demand_df.loc[demand_df['ymdh']==ymdh, 'del_effect_COVID1'] = del_effect_COVID1
    demand_df.loc[demand_df['ymdh']==ymdh, 'effect_COVID2'] = effect_COVID2
    demand_df.loc[demand_df['ymdh']==ymdh, 'del_effect_COVID2'] = del_effect_COVID2
    demand_df.loc[demand_df['ymdh']==ymdh, 'effect_HOLI'] = effect_HOLI
    demand_df.loc[demand_df['ymdh']==ymdh, 'del_effect_HOLI'] = del_effect_HOLI
    demand_df.loc[demand_df['ymdh']==ymdh, 'effect_WEATHER1'] = effect_WEATHER1
    demand_df.loc[demand_df['ymdh']==ymdh, 'del_effect_WEATHER1'] = del_effect_WEATHER1
    demand_df.loc[demand_df['ymdh']==ymdh, 'effect_WEATHER2'] = effect_WEATHER2
    demand_df.loc[demand_df['ymdh']==ymdh, 'del_effect_WEATHER2'] = del_effect_WEATHER2
    demand_df.loc[demand_df['ymdh']==ymdh, 'effect_PAST'] = effect_PAST
    demand_df.loc[demand_df['ymdh']==ymdh, 'effect_PAST2'] = effect_PAST2


    # 기존 과거 데이터에 한줄 추가(past_df)
    past_df = pd.concat([past_df,demand_df.iloc[i:i+1,:]])

    pre_df = past_df.copy()
    pre_df = pre_df.reset_index().drop('index', axis=1)
    pre_df['영업종료시간'] = pre_df['영업종료시간'].apply(lambda x : get_close_time(x))
    pre_df['영업종료시간'] = pre_df['영업종료시간'].replace('24','00')
    pre_df['timegrp']=pre_df['timegrp'].apply(lambda x : get_timegrp(x))

past_df = past_df.reset_index().drop('index', axis=1)
# 최종버전 DB_data 저장
print('Save_final_DB_data')
past_df.to_csv('/home/elap/genieus_ai/DB_data/2021_2022_final_DB_fc.csv', encoding='utf-8-sig')

######## 모델링 실행(R 코드) ####################
import os
# os.system('chcp 65001')
os.system("/usr/bin/Rscript /home/elap/genieus_ai/source/Modeling_fr.R")

####### 지역별 비율 설정 #########################
# 1. 모델링 데이터 전처리
print('Model_data_preprocessing')
## 1) 모델링 결과 데이터 불러오기
result = pd.read_csv("/home/elap/genieus_ai/Result/modeling_result.csv", index_col = 0)

## 2) 모델링 데이터 파생 변수 및 타입 변경
# tmp_result = result[result['ymdh'] == "2022-02-10 21:30:00"]  # 실시간 적용 시 제거
tmp_result = result.copy()
tmp_result = tmp_result.rename(columns = {'pred_lmrf' : 'pred_y'})
tmp_result['ymdh'] = pd.to_datetime(tmp_result['ymdh'])
tmp_result['HOUR'] = tmp_result['ymdh'].dt.hour
tmp_result['WDAY'] = tmp_result['ymdh'].dt.weekday
tmp_result = tmp_result.reset_index().drop('index',axis=1)
math_value = math.ceil(tmp_result['pred_y'][0])
tmp_result.loc[0:1,('pred_y')] =math_value
wday = tmp_result['WDAY'][0]
hour = tmp_result['HOUR'][0]
holiday_yn = tmp_result['holiday_yn'][0]
print(tmp_result)

# 2. 누적 데이터 전처리
print('old_data_preprocessing')
## 1) 누적 데이터 불러오기
old_df = Service_OLD_DB()
sector_demand = old_df.service_section_info(wday, hour)
del old_df
## 2) 누적된 서비스 데이터 중복호출 제거
nodup = RM_duplicate(sector_demand)
origin_nodup = nodup.get_nodup()
del nodup
print('Total number of call:',len(sector_demand))
print('number of duplicated call:',len(sector_demand) - len(origin_nodup))
print('After remove duplicated call:', len(origin_nodup))

# 실시간 코드시 제외
# origin_nodup = origin_nodup[origin_nodup['이용일시'].dt.strftime('%Y-%m-%d %H:%m:%s') < "2022-02-09 21:30:00"]

#지역별 15분 단위 카운트
origin_nodup['ymdh']=origin_nodup['이용일시'].dt.floor(freq="15T")
sector_y = origin_nodup.groupby(['ymdh','sector'])['이용일시'].count().reset_index().rename(columns = {'이용일시':'y'})

# 3. 모델링 변수 누적 데이터 전처리
print('Preprocessing modeling_data')
## 1) 데이터 로드
past_df = pd.read_csv('/home/elap/genieus_ai/DB_data/2021_2022_final_DB_fc.csv', encoding='utf-8-sig', index_col=0)
sel_col = ['ymdh','holiday_yn', 'HOUR']

merge_df = past_df[sel_col]
merge_df['ymdh'] = pd.to_datetime(merge_df['ymdh'])
print('merge_df', merge_df)

## 2) 데이터 병합
past_y = pd.merge(sector_y, merge_df, how='left', on='ymdh')

# print('past_y_1',past_y)
past_y = past_y[past_y['holiday_yn'] == holiday_yn]
past_y = past_y.reset_index().drop('index',axis=1)

# print('past_y', past_y)
# print('sector_y',sector_y)

# 4. 지역별 비율 구하기
print('Get_region_rate')
## 1) 시간별, 지역별 임시 비율 구하기
sector_rate = past_y.groupby(['HOUR','sector'])['y'].sum().reset_index()
sector_rate['rate'] = (sector_rate['y'] / sector_rate['y'].sum())

print(sector_rate)

## 2) control_section_data load
old_df = Service_OLD_DB()
sector_df = old_df.get_control_section()
del old_df

## 3) 0.5 미만은 비율을 0으로 변경
def get_quantile(rate):
    new_rate = 0
    if rate >= sector_prob['rate'].quantile(q=0.95):
        new_rate = rate
    else:
        new_rate = 0
    return new_rate

## 4) 최종비율 적용
sector_prob = pd.merge(sector_df,sector_rate[['sector','rate']], how='left', on='sector')
sector_prob['ymdh'] = tmp_result['ymdh'][0]
sector_prob['rate'] = sector_prob['rate'].fillna(0)
sector_prob['new_rate'] = sector_prob['rate'].apply(lambda x : get_quantile(x))
sector_prob['new_rate'] = sector_prob['new_rate'] / sector_prob['new_rate'].sum()
print(sector_prob.info())
sector_prob['y_pred'] = (round(sector_prob['new_rate'] * tmp_result['pred_y'][0],0)).astype('int')
print("y_pred.max()",sector_prob['y_pred'].max())
print("y_pred.sum()", sector_prob['y_pred'].sum())
print('finish_applying_region_rate')

# 5. Sector_DB update
print('start_db_update')
from Sector_DB_UPdate import Sector_DBUpdate
up_db = Sector_DBUpdate()
up_db.demand_update(sector_prob)
del up_db
print('update_finish')

# 6. 섹터별 결과 저장

old_prob = pd.read_csv('/home/elap/genieus_ai/Result/result_by_sector.csv')
total_result = pd.concat([old_prob, sector_prob])
print(total_result.tail())
total_result = total_result.reset_index().drop('index',axis=1)

total_result.to_csv('/home/elap/genieus_ai/Result/result_by_sector.csv', encoding='utf-8-sig', index = False)
# sector_prob.to_csv('../Result/result_by_sector.csv', encoding='utf-8-sig', index = False)

