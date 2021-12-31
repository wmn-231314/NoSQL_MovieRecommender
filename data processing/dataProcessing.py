import pandas as pd

import csv  # 导入pandas包

data = pd.read_csv("ratings_trian.csv")  # 读取csv文件

dateMap = []

for i in range(len(data)):
    dateMap.append(data["movieID"][i])

print("去重复前数量：" + len(data).__str__())
formatList = list(set(dateMap))
formatList.sort(key=dateMap.index)

print("去重复后数量：" + len(formatList).__str__())

# with open('result2.csv', 'w') as csvfile:
#     writer = csv.DictWriter(csvfile)
#     # writer.writeheader()
#     for i in range(len(formatList)):
#         writer.writerow(formatList)
#         # writer.writerow({'userId': formatList[i]})

f = open('test.csv','a',encoding='utf8',newline='')
writer = csv.writer(f)  # csv.writer()中可以传一个文件对象 # 该data既可以是列表嵌套列表的数据类型也可以是列表嵌套元组的数据类型
writer.writerow(formatList)

