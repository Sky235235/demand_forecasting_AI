import pandas as pd
import pymysql

class Service_OLD_DB:
    # DB 연결 세팅
    def __init__(self):

        host = '****'
        username = '****'
        pw = '*****'
        port = '****'
        db = '****'

        self.conn = pymysql.connect(host=host, user=username, db=db, port=port, password=pw)

    # 관측데이터로 학습할 데이터 로드 작업 # 코드 수정 필요 vulk 어제 날짜 조회로 코드 수정
    def ob_service_data(self):
        query = """

            SELECT  ****
    
            FROM a.table
            WHERE date_format(a.datetime, '%Y-%m-%d') BETWEEN '2022-10-18' AND '2022-11-20'  
            ORDER BY a.datetime"""

        curs = self.conn.cursor(pymysql.cursors.DictCursor)
        curs.execute(query)
        data = pd.DataFrame(curs.fetchall())
        return data

    # service_section_info : 지역별 비율 설정
    def service_section_info(self, wday, hour):

        query = """
            SELECT ****"
                   
            FROM im_service_section_info a
            LEFT JOIN im_service_car_log_info b ON a.idx = b.idx
            WHERE b.boarding_week = {} AND b.boarding_hour = {} AND a.section <> 9999""".format(wday, hour)

        curs = self.conn.cursor(pymysql.cursors.DictCursor)
        curs.execute(query)
        data = pd.DataFrame(curs.fetchall())

        return data

    # weather Ob data
    def weather_info(self): # 배포시 어제 날짜 #관측데이터로 학습시킬 시 필요, #vulk 데이터 어제 날짜 조회로 코드 수정
        query = """
        SELECT DATE_FORMAT(a.reg_datetime, '%Y-%m-%d') AS 'DATE',
               HOUR(a.reg_datetime) AS 'HOUR',
               a.temperature AS '평균기온',
               a.rainfall AS '평균강수량'

        FROM `pos`.`im_service_weather_info` a
        WHERE DATE_FORMAT(a.reg_datetime, '%Y-%m-%d') BETWEEN '2022-10-18' AND '2022-11-20'  
        ORDER BY a.reg_datetime """

        curs = self.conn.cursor(pymysql.cursors.DictCursor)
        curs.execute(query)
        data = pd.DataFrame(curs.fetchall())

        return data

    def get_control_section(self):
        query = """
                SELECT a.section as "sector"
                FROM control_section_data a"""

        curs = self.conn.cursor(pymysql.cursors.DictCursor)
        curs.execute(query)
        data = pd.DataFrame(curs.fetchall())

        return data

    # DB 연결 해제
    def __del__(self):
        curs = self.conn.cursor(pymysql.cursors.DictCursor)
        curs.close()
        self.conn.close()
