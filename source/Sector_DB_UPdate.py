import pandas as pd
import pymysql
from tqdm import tqdm

class Sector_DBUpdate:
    def __init__(self):
        ## LIVE 배차 (server 접근)
        host = '*****'
        username = '******'
        pw = '***'
        port = '****'
        db = '****'

        self.conn = pymysql.connect(host=host, user=username, db=db, port=port, password=pw)

    def demand_update(self, df):
        curs = self.conn.cursor(pymysql.cursors.DictCursor)

        pred_list = list(df['y_pred'])
        sector_list = list(df['sector'])

        for i in tqdm(range(len(pred_list))):
            query = """
                     UPDATE control_section_data
                     SET traffic = {}
                     WHERE section = {}
               """.format(pred_list[i], sector_list[i])
            curs.execute(query)

            self.conn.commit()

        # 소멸자

    def __del__(self):
        curs = self.conn.cursor(pymysql.cursors.DictCursor)
        curs.close()
        self.conn.close()


