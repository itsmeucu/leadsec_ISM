import math

import requests
import json
import os
import urllib3
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

'''
windows运行时，提前在命令提示符运行
set PYTHONWARNINGS="ignore:Unverified HTTPS request"


linux运行时，提取在终端运行
export PYTHONWARNINGS="ignore:Unverified HTTPS request"


'''

def get_next_page_number():
    # 列出当前目录下所有的.json文件
    files = [f for f in os.listdir('.') if f.endswith('.json')]

    # 提取页码并找到最大的页码
    max_page = 0
    for file in files:
        page_number = int(file.split('.')[0])
        if page_number > max_page:
            max_page = page_number

    # 返回下一页的页码
    return max_page + 1



def calculate_total_pages(total_count, items_per_page):
    return math.ceil(total_count / items_per_page)

def getInfo(page = 1 ,PHPSESSID = '',starttime = '', endtime = '', serverIP = ''):
    if PHPSESSID == '' or starttime == '' or endtime == ''  or PHPSESSID is None or starttime is None or endtime is None:
        return None
    try:

        cookies = {
            'PHPSESSID': PHPSESSID,
        }

        headers = {
            'Connection': 'keep-alive',
            'Accept': '*/*',
            'X-Requested-With': 'XMLHttpRequest',
            'User-Agent': 'Mozilla/5.0 (Windows NT 6.1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/86.0.0.0 Safari/537.36',
            'Content-Type': 'application/x-www-form-urlencoded; charset=UTF-8',
            'Origin': 'https://'+serverIP+':8834',
            'Sec-Fetch-Site': 'same-origin',
            'Sec-Fetch-Mode': 'cors',
            'Sec-Fetch-Dest': 'empty',
            'Referer': 'https://'+serverIP+':8834/Report/RepVirus',
            'Accept-Language': 'zh-CN,zh;q=0.9',
        }

        params = {
            'tokenmu': 'hpeSs+oERXROdJeAT6mlC/u2JrgQxk3gXJPfs7A71Kg=',
        }

        data = {
            'type': '1',
            'starttime': starttime,
            'endtime': endtime,
            'isquery': '1',
            'rp': '200',
            'page': 1,
            'sortname': 'occurtime',
            'sortorder': 'DESC',
        }

        response = requests.post(
            'https://'+serverIP+':8834/RepVirus/GetScanLog',
            params=params,
            cookies=cookies,
            headers=headers,
            data=data,
            verify=False,
        )
        with open(str(page) + '.json', 'w', encoding='utf-8') as f:
            f.write(json.dumps(response.json(), ensure_ascii=False, indent=4))
        return response.json()
    except Exception as e:
        print(e)
        return None

if __name__ == '__main__':
    serverIP = input("Server IP: ")
    if serverIP == '':
        serverIP = '127.0.0.1'
    PHPSESSID = input("PHPSESSID:")
    print("Start Time Formate:\t 2024-12-01 00:00:00")
    print("End Time Formate:\t 2024-12-31 23:23:23")
    starttime = input("starttime:")
    endtime = input("endtime:")

    # serverIP = '127.0.0.1'
    # PHPSESSID = 'kkkkkkkk'
    # starttime = '2024-12-01 00:00:00'
    # endtime = '2024-12-31 23:23:23'


    isInfo = getInfo(1, PHPSESSID=PHPSESSID,starttime=starttime,endtime=endtime,serverIP=serverIP)

    if isInfo is None:
        exit(0)

    total_number = isInfo['total']
    total_page = calculate_total_pages(total_number,200)
    start_page = get_next_page_number()


    # 如果起始页码大于总页数，说明数据已经全部获取完毕
    if start_page > total_page:
        print("All pages have been fetched.")
        print("所有页数据获取完毕")
    else:
        for page in range(start_page, total_page + 1):
            print(f'当前页 {page}',f'总页数 {total_page}')
            getInfo(page=page,PHPSESSID=PHPSESSID,starttime=starttime,endtime=endtime,serverIP=serverIP)