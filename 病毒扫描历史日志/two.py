'''
数据合并
'''
import json
import glob
import os
import sqlite3
import time

def add_to_json_data():
    '''
    合并rows中的数据并返回
    :return: rows_data
    '''
    # 使用glob模块获取当前文件夹下所有.json文件的列表
    file_list = glob.glob('*.json')

    # 对文件列表进行排序，确保它们是按照文件名顺序处理的
    file_list.sort()

    # 初始化一个空的列表来存储合并后的rows数据
    rows_data = []

    # 遍历文件列表并合并数据
    for file_name in file_list:
        # 读取文件内容
        with open(file_name, 'r', encoding='utf-8') as file:
            file_data = json.load(file)

        # 确保文件数据中包含rows键
        if "rows" in file_data:
            # 将当前文件的rows数据添加到rows_data列表中
            rows_data.extend(file_data["rows"])

    # 返回合并后的rows数据
    return rows_data


def add_to_json_file(filename='combined_data'):
    '''
    合并到一个json文件
    :return:
    '''
    combined_data = add_to_json_data()
    # 如果需要，可以将合并后的数据写入到一个新的JSON文件中
    with open(f'{filename}.json', 'w', encoding='utf-8') as output_file:
        json.dump(combined_data, output_file, ensure_ascii=False, indent=4)


def create_sqlite():
    conn = sqlite3.connect('virus_data.db')
    # 创建一个游标对象来执行SQL命令
    cursor = conn.cursor()

    # 创建表（如果表不存在的话）
    create_table_query = '''
        CREATE TABLE IF NOT EXISTS virus_logs (
            id TEXT,
            logid TEXT,
            taskid TEXT,
            virusname TEXT,
            virustype TEXT,
            virustypedesc TEXT,
            viruspath TEXT,
            virusmd5 TEXT,
            occurtime TEXT,
            cleantime TEXT,
            issync TEXT,
            eventtype TEXT,
            filesize TEXT,
            fileorigin TEXT,
            opresult TEXT,
            osversion TEXT,
            guid TEXT,
            hostname TEXT,
            ip TEXT,
            mask TEXT,
            mac TEXT,
            winuser TEXT,
            depnamelevel1 TEXT,
            depnamelevel2 TEXT,
            depnamelevel3 TEXT,
            depnamelevel4 TEXT,
            depnamelevel5 TEXT,
            serverip TEXT,
            logtime TEXT,
            engine TEXT,
            depname TEXT,
            enginetype TEXT
        );
        '''
    cursor.execute(create_table_query)


def save_to_sqlite_one(data, isRnew=False):
    # 连接到SQLite数据库（如果数据库不存在，则会自动创建）
    if not os.path.exists("virus_data.db") or isRnew:
        create_sqlite()
    conn = sqlite3.connect('virus_data.db')

    # 创建一个游标对象来执行SQL命令
    cursor = conn.cursor()

    # 插入数据
    insert_data_query = '''
    INSERT INTO virus_logs (
        id, logid ,taskid ,virusname ,virustype ,virustypedesc ,viruspath ,virusmd5 ,occurtime ,cleantime ,issync ,eventtype ,filesize ,fileorigin ,opresult ,osversion ,guid ,hostname ,ip ,mask ,mac ,winuser ,depnamelevel1 ,depnamelevel2 ,depnamelevel3 ,depnamelevel4 ,depnamelevel5 ,serverip ,logtime ,engine ,depname ,enginetype
    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?,?,?,?,?)
    '''

    # 确保 data 字典中的键与插入查询的列名完全匹配，并填充空值
    other_id = data['id']
    data = data['cell']
    cursor.execute(insert_data_query, (
        other_id,
        data['logid'],
        data['taskid'] if 'taskid' in data else '',
        data['virusname'] if 'virusname' in data else '',
        data['virustype'] if 'virustype' in data else '',
        data['virustypedesc'] if 'virustypedesc' in data else '',
        data['viruspath'] if 'viruspath' in data else '',
        data['virusmd5'] if 'virusmd5' in data else '',
        data['occurtime'] if 'occurtime' in data else '',
        data['cleantime'] if 'cleantime' in data else '',
        data['issync'] if 'issync' in data else '',
        data['eventtype'] if 'eventtype' in data else '',
        data['filesize'] if 'filesize' in data else '',
        data['fileorigin'] if 'fileorigin' in data else '',
        data['opresult'] if 'opresult' in data else '',
        data['osversion'] if 'osversion' in data else '',
        data['guid'] if 'guid' in data else '{}',
        data['hostname'] if 'hostname' in data else '',
        data['ip'] if 'ip' in data else '',
        data['mask'] if 'mask' in data else '',
        data['mac'] if 'mac' in data else '',
        data['winuser'] if 'winuser' in data else '',
        data['depnamelevel1'] if 'depnamelevel1' in data else '',
        data['depnamelevel2'] if 'depnamelevel2' in data else '',
        data['depnamelevel3'] if 'depnamelevel3' in data else '',
        data['depnamelevel4'] if 'depnamelevel4' in data else '',
        data['depnamelevel5'] if 'depnamelevel5' in data else '',
        data['serverip'] if 'serverip' in data else '',
        data['logtime'] if 'logtime' in data else '',
        data['engine'] if 'engine' in data else '',
        data['depname'] if 'depname' in data else '',
        data['enginetype'] if 'enginetype' in data else ''
    ))

    # 提交事务
    conn.commit()

    # 关闭连接
    conn.close()

def save_to_sqlite_some(datas, isRnew=False):
    # 连接到SQLite数据库（如果数据库不存在，则会自动创建）
    if not os.path.exists("virus_data.db") or isRnew:
        create_sqlite()
    conn = sqlite3.connect('virus_data.db')

    # 创建一个游标对象来执行SQL命令
    cursor = conn.cursor()

    for data in datas:
        # 插入数据
        insert_data_query = '''
        INSERT INTO virus_logs (
            id, logid ,taskid ,virusname ,virustype ,virustypedesc ,viruspath ,virusmd5 ,occurtime ,cleantime ,issync ,eventtype ,filesize ,fileorigin ,opresult ,osversion ,guid ,hostname ,ip ,mask ,mac ,winuser ,depnamelevel1 ,depnamelevel2 ,depnamelevel3 ,depnamelevel4 ,depnamelevel5 ,serverip ,logtime ,engine ,depname ,enginetype
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?,?,?,?,?)
        '''

        # 确保 data 字典中的键与插入查询的列名完全匹配，并填充空值
        other_id = data['id']
        data = data['cell']
        cursor.execute(insert_data_query, (
            other_id,
            data['logid'],
            data['taskid'] if 'taskid' in data else '',
            data['virusname'] if 'virusname' in data else '',
            data['virustype'] if 'virustype' in data else '',
            data['virustypedesc'] if 'virustypedesc' in data else '',
            data['viruspath'] if 'viruspath' in data else '',
            data['virusmd5'] if 'virusmd5' in data else '',
            data['occurtime'] if 'occurtime' in data else '',
            data['cleantime'] if 'cleantime' in data else '',
            data['issync'] if 'issync' in data else '',
            data['eventtype'] if 'eventtype' in data else '',
            data['filesize'] if 'filesize' in data else '',
            data['fileorigin'] if 'fileorigin' in data else '',
            data['opresult'] if 'opresult' in data else '',
            data['osversion'] if 'osversion' in data else '',
            data['guid'] if 'guid' in data else '{}',
            data['hostname'] if 'hostname' in data else '',
            data['ip'] if 'ip' in data else '',
            int_to_subnet_mask(data['mask']) if 'mask' in data else '',
            data['mac'] if 'mac' in data else '',
            data['winuser'] if 'winuser' in data else '',
            data['depnamelevel1'] if 'depnamelevel1' in data else '',
            data['depnamelevel2'] if 'depnamelevel2' in data else '',
            data['depnamelevel3'] if 'depnamelevel3' in data else '',
            data['depnamelevel4'] if 'depnamelevel4' in data else '',
            data['depnamelevel5'] if 'depnamelevel5' in data else '',
            int_to_subnet_mask(data['serverip']) if 'serverip' in data else '',
            data['logtime'] if 'logtime' in data else '',
            data['engine'] if 'engine' in data else '',
            data['depname'] if 'depname' in data else '',
            data['enginetype'] if 'enginetype' in data else ''
        ))

    # 提交事务
    conn.commit()

    # 关闭连接
    conn.close()


def save_to_sqlite_some_one_by_one(datas, isRnew=False):
    for data in datas:
        save_to_sqlite_one(data,isRnew)

def int_to_subnet_mask(mask_int):
    try:
        # 将整数转换为32位二进制字符串
        binary_str = f'{mask_int:032b}'
        # 将二进制字符串分割为4个8位的部分
        octets = [binary_str[i:i+8] for i in range(0, 32, 8)]
        # 将每个8位部分转换为十进制并连接成点分十进制格式
        subnet_mask = '.'.join(str(int(octet, 2)) for octet in octets)
        return subnet_mask
    except ValueError:
        return mask_int


if __name__ == '__main__':
    # create_database()
    # insert_database(add_to_json_data())
    # print(add_to_json_data())
    time1 = time.time()
    save_to_sqlite_some(add_to_json_data(), isRnew=True)
    time2 = time.time()
    print("Done")
    print(time2 - time1)
