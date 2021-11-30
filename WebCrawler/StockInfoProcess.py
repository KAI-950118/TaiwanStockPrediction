import matplotlib.pyplot as plt
import pandas as pd
import numpy as np

plt.rcParams['font.sans-serif'] = ['SimHei']  # 用來正常顯示中文標籤
plt.rcParams['axes.unicode_minus'] = False  # 用來正常顯示負號
# plt.rcParams.update({'font.size': 10})

'Load Data'
stock_total = pd.read_excel('stock_total.xlsx')
print(stock_total.head())

'所有台股種類'
stock_total_type_temp = '水泥工業	食品工業	塑膠工業	紡織纖維	電機機械 電器電纜	生技醫療業	化學工業	玻璃陶瓷	造紙工業 鋼鐵工業	橡膠工業	汽車工業	電腦及週邊設備業	半導體業 電子零組件業	其他電子業	通信網路業	資訊服務業	建材營造業 航運業	觀光事業	銀行業	保險業	金控業 貿易百貨業	光電業	電子通路業	證券業	其他業 油電燃氣業	電子商務	文化創意業	農業科技業'
stock_total_type = stock_total_type_temp.split()
# print(stock_total_type)
print(f'Total Type number in stock {len(stock_total_type)}')

'判斷DB內有多少種種類'
stock_type = list(stock_total['產業別'])
stock_type_uni = np.unique(stock_type)
# print(stock_type_uni)
print(f'Total Type number in stock database {len(stock_type_uni)}')

'確認沒有在DB內的種類'
stock_type_not_in_data = []
for type_check in stock_total_type:
    if type_check not in stock_type_uni:
        stock_type_not_in_data.append(type_check)
print('These type not in database:', stock_type_not_in_data)

'計算各種類個數'
stock_type_count = []
for uni in stock_type_uni:
    stock_type_count.append(stock_type.count(uni))
    # print(f'{uni}: {stock_type.count(uni)}')
# print(stock_type_count)

'尋找內容過少的類別'
stock_type_too_few = []
for i in range(len(stock_type_count)):
    if stock_type_count[i] <= 1:
        stock_type_too_few.append(stock_type_uni[i])
print('These type have too few stock:', stock_type_too_few)
# plt.bar(stock_type_uni,
#         stock_type_count)
# plt.xticks(rotation=45)
# plt.show()



