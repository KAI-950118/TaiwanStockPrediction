import pandas as pd
import requests
from bs4 import BeautifulSoup
import pymysql
import time

# 資料庫參數設定
db_settings = {
    "host": "127.0.0.1",
    "port": 3306,
    "user": "root",
    "password": "root",
    "db": "aiforstock",
    "charset": "utf8"
}

####################################################################################################################################################################################

# 從 台灣證券交易所 爬蟲
# # import xlwings as xw
#
# # print(pd.__version__)
# # print(requests.__version__)
# # print(xw.__version__)
#
# # 會被懲罰 會很久
# url = 'https://www.twse.com.tw/exchangeReport/STOCK_DAY?response=csv&date=20210801&stockNo=2308'
# res = requests.get(url)
# data = res.text
# # print(data)
#
# cleaned_data = []
# for da in data.split('\n'):
#     if len(da.split('","')) == 9 and da.split('","')[0][0] != '=':
#         cleaned_data.append([ele.replace('",\r','').replace('"','')
#
#                              for ele in da.split('","')])
#
# @@@df = pd.DataFrame(cleaned_data[1:], columns = ['date','numberOfTraded','moneyOfTraded','openingPrice','highestPrice','lowestPrice','closingPrice']) #columns = cleaned_data[0]
# print(df)

####################################################################################################################################################################################
# 從Goodinfo爬蟲
# 建立Connection物件
conn_for_PN = pymysql.connect(**db_settings)

# 建立Cursor物件
with conn_for_PN.cursor() as cursor_for_PN:
    # 新增資料指令
    command_for_PN = "SELECT stockPN FROM stock_info WHERE stockPN > '2603'"  # WHERE stockPN = '3714'
    # 執行指令
    cursor_for_PN.execute(command_for_PN)
    # 取得第一筆資料
    result = cursor_for_PN.fetchall()
    # print(result)

# 整理所有STOCK出來
stockpn_4_list = []
for temp_PN in result:
    stockpn_4_list.append(temp_PN[0])
# print(stockpn_4_list)

# 批量行動
# To SQL
try:
    # 建立Connection物件
    conn = pymysql.connect(**db_settings)
    # 建立Cursor物件
    with conn.cursor() as cursor:
        for stockpn_4 in stockpn_4_list:
            print(stockpn_4)

            # 假裝是人
            headers = {
                'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/96.0.4664.45 Safari/537.36'
            }
            # 張數
            sheet_data = 'https://goodinfo.tw/StockInfo/EquityDistributionClassHis.asp?STOCK_ID=' + str(
                stockpn_4) + '&DISPLAY_CAT=%E6%8C%81%E6%9C%89%E5%BC%B5%E6%95%B8%E5%8D%80%E9%96%93%E5%88%86%E7%B4%9A%E4%B8%80%E8%A6%BD(%E7%B0%A1%E5%8C%96)&CHT_CAT=MONTH'
            # 比例
            rate_data = 'https://goodinfo.tw/StockInfo/EquityDistributionClassHis.asp?STOCK_ID=' + str(
                stockpn_4) + '&DISPLAY_CAT=%E6%8C%81%E6%9C%89%E6%AF%94%E4%BE%8B%E5%8D%80%E9%96%93%E5%88%86%E7%B4%9A%E4%B8%80%E8%A6%BD%28%E7%B0%A1%E5%8C%96%29&CHT_CAT=MONTH'

            res = requests.get(rate_data, headers=headers)
            res.encoding = 'utf-8'
            # print(res.text)
            soup = BeautifulSoup(res.text, 'lxml')
            data = soup.select_one('#divEquityDistributionClassHis')
            pd_data = pd.read_html(data.prettify())
            # print(len(pd_data))
            pd_final_data = pd_data[2]

            res_sheet = requests.get(sheet_data, headers=headers)
            res_sheet.encoding = 'utf-8'
            # print(res_sheet.text)
            soup_sheet = BeautifulSoup(res_sheet.text, 'lxml')
            data_sheet = soup_sheet.select_one('#divEquityDistributionClassHis')
            pd_data_sheet = pd.read_html(data_sheet.prettify())
            # print(len(pd_data))
            pd_final_data_sheet = pd_data_sheet[2]

            print(time.asctime(time.localtime(time.time())))

            pd_final_data.columns = ['yearmonth', 'staDate', 'price', 'dollar', 'percent', '10', '10-50', '50-100',
                                     '100-200',
                                     '200-400', '400-800', '800-1000', '1000', 'A', 'B', 'C', 'D', 'E', 'F', 'G',
                                     'H', 'I']
            pd_final_data_sheet.columns = ['yearmonth', 'staDate', 'price', 'dollar', 'percent', '10', '10-50',
                                           '50-100',
                                           '100-200',
                                           '200-400', '400-800', '800-1000', '1000', 'A', 'B', 'C', 'D', 'E', 'F', 'G',
                                           'H', 'I']
            # 刪除不要的Column
            pd_final_data = pd_final_data.drop(
                columns=['staDate', 'price', 'dollar', 'percent', 'A', 'B', 'C', 'D', 'E', 'F', 'G', 'H', 'I'])

            pd_final_data_sheet = pd_final_data_sheet.drop(
                columns=['staDate', 'price', 'dollar', 'percent', 'A', 'B', 'C', 'D', 'E', 'F', 'G', 'H', 'I'])
            # print(pd_final_data_sheet.columns)

            # 刪除不要的Row
            row_delete = []
            for index, row_check in enumerate(pd_final_data.yearmonth):
                if "M" not in row_check:
                    row_delete.append(index)
                    # print(index, row_check)
            pd_final_data = pd_final_data.drop(row_delete)
            pd_final_data_sheet = pd_final_data_sheet.drop(row_delete)

            # 重新塑形PD
            pd_final_data['stockPN'] = stockpn_4
            pd_final_data['recordYear'] = pd_final_data.yearmonth.apply(lambda x: '20' + str(x[0: 2]))
            pd_final_data['recordMonth'] = pd_final_data.yearmonth.apply(lambda x: int(x[3:]))

            pd_final_data = pd_final_data.drop(columns=['yearmonth'])

            pd_final_data_sheet['stockPN'] = stockpn_4
            pd_final_data_sheet['recordYear'] = pd_final_data_sheet.yearmonth.apply(lambda x: '20' + str(x[0: 2]))
            pd_final_data_sheet['recordMonth'] = pd_final_data_sheet.yearmonth.apply(lambda x: int(x[3:]))

            pd_final_data_sheet = pd_final_data_sheet.drop(columns=['yearmonth'])

            # print(pd_final_data.head(20))

            # # To EXCEL驗證
            # Result = 'D:\AI\Stock_Prediction\SAMPLE.csv'
            # df_SAMPLE = pd.DataFrame.from_dict(pd_final_data_sheet)
            # df_SAMPLE.to_csv(Result, index=False)

            # # 新增資料SQL語法

            for index, temp_data in pd_final_data.iterrows():
                temp_data_sheet = pd_final_data_sheet.loc[index]
                # print(temp_data_sheet)
                command = "INSERT INTO number_of_issued_shares(`stockPN`, `recordYear`, `recordMonth`, `SHratio_lower10`, `SHratio_10-50`, `SHratio_50-100`, `SHratio_100-200`, `SHratio_200-400`, `SHratio_400-800`, `SHratio_800-1000`, `SHratio_upper1000`) " \
                          "VALUES(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s) ON DUPLICATE KEY UPDATE `SHsheets_lower10` = %s, `SHsheets_10-50` = %s, `SHsheets_50-100` = %s, `SHsheets_100-200` = %s, `SHsheets_200-400` = %s, `SHsheets_400-800` = %s, `SHsheets_800-1000` = %s, `SHsheets_upper1000` = %s ,`aveNumofIssuedShares` = %s;"  # 比例

                if temp_data['10'] != '-':
                    # 避免除以0
                    sum_temp = []
                    for temp_column in ['10', '10-50', '50-100', '100-200', '200-400', '400-800', '800-1000', '1000']:
                        if float(temp_data[temp_column]) != 0:
                            sum_temp.append(float(temp_data_sheet[temp_column]) / float(temp_data[temp_column]))
                    avg_temp = sum(sum_temp) / len(sum_temp) * 100 * 10000
                    # print(avg_temp)
                    cursor.execute(
                        command, (temp_data["stockPN"], temp_data["recordYear"], int(temp_data["recordMonth"]),
                                  float(temp_data["10"]), float(temp_data["10-50"]), float(temp_data["50-100"]),
                                  float(temp_data["100-200"]),
                                  float(temp_data["200-400"]), float(temp_data["400-800"]),
                                  float(temp_data["800-1000"]), float(temp_data["1000"]),
                                  float(temp_data_sheet["10"]), float(temp_data_sheet["10-50"]),
                                  float(temp_data_sheet["50-100"]),
                                  float(temp_data_sheet["100-200"]),
                                  float(temp_data_sheet["200-400"]), float(temp_data_sheet["400-800"]),
                                  float(temp_data_sheet["800-1000"]),
                                  float(temp_data_sheet["1000"]), int(avg_temp)))
            ## 儲存變更
            conn.commit()

            ## 延遲避免爬太快
            time.sleep(15)

except Exception as ex:
    print('error: ' + str(ex))
