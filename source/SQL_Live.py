import pandas as pd
import pymysql

class Service_LIVE_DB:
    # DB 연결 세팅
    def __init__(self):

        host = '***'
        username = '***'
        pw = '****'
        port = '****'
        db = '*****'  # 실시간 데이터
        self.conn = pymysql.connect(host=host, user=username, db=db, port=port, password=pw)

    # Load service_data
    def service_data(self):  # Live_data 적용 #30분 단위 데이터 추출
        # conn = self.conn
        query = """
              SELECT  *

                FROM im_mobility.boarding_history a LEFT JOIN im_mobility.user b ON a.user_idx = b.user_idx
                WHERE a.reg_datetime >= DATE_FORMAT(FROM_UNIXTIME(TRUNCATE((UNIX_TIMESTAMP(NOW()) - 900) / 900, 0) * 900), '%Y-%m-%d %H:%i:%s') 
                AND a.reg_datetime < DATE_FORMAT(FROM_UNIXTIME(TRUNCATE(UNIX_TIMESTAMP(NOW()) / 900, 0) * 900), "%Y-%m-%d %H:%i:%s")

                UNION ALL
                SELECT *
                FROM im_mobility.reservation_boarding_history a LEFT JOIN im_mobility.user b ON a.user_idx = b.user_idx
                WHERE a.is_cancel = 'Y' 
                AND a.reservation_datetime >= DATE_FORMAT(FROM_UNIXTIME(TRUNCATE((UNIX_TIMESTAMP(NOW()) - 900) / 900, 0) * 900), '%Y-%m-%d %H:%i:%s')
                AND a.reservation_datetime < DATE_FORMAT(FROM_UNIXTIME(TRUNCATE(UNIX_TIMESTAMP(NOW()) / 900, 0) * 900), "%Y-%m-%d %H:%i:%s")
                      
                
                UNION ALL

                SELECT *
                FROM im_mobility.general_boarding_history a 
                WHERE a.reg_datetime >= DATE_FORMAT(FROM_UNIXTIME(TRUNCATE((UNIX_TIMESTAMP(NOW()) - 900) / 900, 0) * 900), '%Y-%m-%d %H:%i:%s') 
                AND a.reg_datetime < DATE_FORMAT(FROM_UNIXTIME(TRUNCATE(UNIX_TIMESTAMP(NOW()) / 900, 0) * 900), "%Y-%m-%d %H:%i:%s")
                ORDER BY 이용일시;"""

        # data = pd.read_sql(query, conn)
        curs = self.conn.cursor(pymysql.cursors.DictCursor)
        curs.execute(query)
        data = pd.DataFrame(curs.fetchall())

        return data
    
    # DB 연결 해제
    def __del__(self):
        curs = self.conn.cursor(pymysql.cursors.DictCursor)
        curs.close()
        self.conn.close()





