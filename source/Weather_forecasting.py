import pandas as pd
import json
import requests
from datetime import date, timedelta
from tqdm import tqdm
import warnings
warnings.filterwarnings(action='ignore')
import re

# 1.격자정보 데이터 불러오기(정기적으로 업데이트 필요)
print('Loading nxy_data')
nxy_df =pd.read_csv('/home/elap/genieus_ai/DB_data/기상청41_단기예보 조회서비스_오픈API활용가이드_격자_위경도(20210401).csv', encoding='cp949')
col = ['1단계','2단계','3단계','격자 X','격자 Y','위도(초/100)','경도(초/100)']
nxy_df = nxy_df[col]

# 2. 서울특별시의 격자리스트 만들기
print('Make nxy_list for Seoul')
nxy_df = nxy_df[nxy_df['1단계'] == '서울특별시']
nxy_list = list(zip(nxy_df['격자 X'].values, nxy_df['격자 Y'].values))
nxy_list = list(set(nxy_list))

# 3. 예보 API 호출
print('Call Weather_forecasting API')
## 1) 오늘날짜 설정
today = date.today()
today = today.strftime('%Y%m%d')
print(today)
## 2) url 및 service_key setting
print('url and service key setting')
url = 'http://apis.data.go.kr/1360000/VilageFcstInfoService_2.0/getVilageFcst'
service_key = '1rTRcPcrgRX4bckCMPyIIgsMgxqzwZwAKsqCjJe74xrEwdc2rQRZgHAZ60aJdhT6313RnB8znsO2jJONz+ltow=='
end_time = str(date.today() + timedelta(3)) + ' ' + '08:00:00'
print(end_time)
weather = pd.DataFrame()

print('API Call each nxy')
# nxy_list에 있는 원소별로 api call
for nxy in tqdm(nxy_list):
    params = {'serviceKey' : service_key,
          'pageNo' : "1",
          'numOfRows' : '1000',
          'dataType' : 'JSON',
          'base_date' : today, #today로 변경
          'base_time' : '0800', # 매일 새벽 8시 기준 (8시 50분에 실행)
          'nx' : str(nxy[0]),
          'ny' : str(nxy[1])}
    res = requests.get(url, params = params)
    data = res.content
    w_f = json.loads(data)
    tmp_df = pd.json_normalize(w_f['response']['body']['items']['item'])
    weather = pd.concat([weather, tmp_df])

weather['nx'] = weather['nx'].astype('str')
weather['ny'] = weather['ny'].astype('str')
weather['nxy']= weather['nx'] + weather['ny']

cat_list = ['TMP','PCP','SNO','WSD']
weather = weather[weather['category'].isin(cat_list)]

col = ['nxy','fcstDate','fcstTime','category','fcstValue']
wea_fcst=weather[col]

wea_fcst['fcstValue']=wea_fcst['fcstValue'].replace('강수없음',0)
wea_fcst['fcstValue']=wea_fcst['fcstValue'].replace('적설없음',0)
wea_fcst = wea_fcst.reset_index().drop('index',axis=1)
wea_fcst['category']=wea_fcst['category'].map({'PCP' : '평균강수량', 'TMP' :'평균기온','SNO':'평균적설량','WSD':'평균풍속'})
wea_fcst['fcstValue'] = wea_fcst['fcstValue'].astype('str')

fcst_value_list = []
for i in tqdm(range(len(wea_fcst))):
    fcst = wea_fcst['fcstValue'][i]
    value = re.findall("-?\d+", fcst)

    if len(value) == 2:
        fcst_value_list.append(".".join(value))
    else:
        fcst_value_list.append(value[0])

wea_fcst['fcstValue'] = fcst_value_list
wea_fcst['fcstValue'] = wea_fcst['fcstValue'].astype('float')

wea_fcst = wea_fcst.pivot_table(index=['nxy','fcstDate','fcstTime'],columns='category',values='fcstValue').reset_index()
wea_fcst['fc_datetime']=pd.to_datetime(wea_fcst['fcstDate']+''+wea_fcst['fcstTime'])
wea_fcst = wea_fcst[wea_fcst['fc_datetime'].dt.strftime('%Y-%m-%d %H:%m:%s') <= end_time]

col = ['fc_datetime', 'nxy','평균강수량','평균기온','평균적설량','평균풍속']
w_fcst_df = wea_fcst[col]
print(w_fcst_df)

# 4. 예보 데이터 전처리 및 저장
print('get weather mean value by hour')
w_fcst_df_mean = w_fcst_df.groupby(['fc_datetime'])['평균강수량','평균기온','평균적설량','평균풍속'].mean().reset_index()
w_fcst_df_mean['DATE'] = w_fcst_df_mean['fc_datetime'].dt.date
w_fcst_df_mean['HOUR'] = w_fcst_df_mean['fc_datetime'].dt.hour
sel_col = ['DATE','HOUR','평균강수량','평균기온','평균적설량','평균풍속']
w_fcst_df_mean = w_fcst_df_mean[sel_col]

print("save w_fcst_df_mean file as an excel")
w_fcst_df_mean.to_excel('/home/elap/genieus_ai/DB_data/weather_forecasting.xlsx', encoding='utf-8-sig', index=False)




