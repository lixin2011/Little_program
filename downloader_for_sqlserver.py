import pymssql
import pandas as pd
import tqdm 
import os

os.chdir(r'D:\temp')

def get_data_from_server(location,month,ip,user,password):
    '''
    输入数据表名上的位置以及月份，返回一个字典组成的列表，以及字典的键列表
    '''
    with pymssql.connect(ip,user,password) as conn:
        with conn.cursor(as_dict=True) as cursor:
            query = 'SELECT * FROM [data].[dbo].[{:s}#站历史曲线M{:s}]'.format(str(location),str(month))
            cursor.execute(query)
            results = cursor.fetchall()
            key_list = list(results[0].keys())
            return results,key_list

def get_points_definition_from_server(ip,user,password):
    '''
    输入数据表上的位置，需要保证为两位，返回一个编号和名称对应的字典，用于获取点定义的信息
    '''
    name_dict = {}
    with pymssql.connect(ip,user,password) as conn:
        with conn.cursor(as_dict=True) as cursor:
            cursor.execute('SELECT [PointName],[PointAlias] FROM [data].[dbo].[点定义表]')
            results = cursor.fetchall()
            for row in results:
                row['PointName'] = row['PointName'].strip()
                name_dict[row['PointName']] = row['PointAlias']
            return name_dict

def export_data_to_csv(data,keys,name_dict,location_str,location,month):
    for key_index in range(len(keys)):
            if (keys[key_index] == 'AI0')|(keys[key_index] == 'AI1')|(keys[key_index] == 'AI2')|(keys[key_index] == 'AI3')|(keys[key_index] == 'AI4')|(keys[key_index] == 'AI5')| \
                (keys[key_index] == 'AI6')|(keys[key_index] == 'AI7')|(keys[key_index] == 'AI8')|(keys[key_index] == 'AI9'):
                keys[key_index] = name_dict[location_str + keys[key_index][:2] + '0' + keys[key_index][-1]]
            elif (keys[key_index] == '年')|(keys[key_index] == '月')|(keys[key_index] == '日')|(keys[key_index] == 'SerialNumber')|(keys[key_index] == 'IDX'):
                pass
            else:
                keys[key_index] = name_dict[location_str+keys[key_index]]
    data_to_csv = pd.DataFrame(columns=keys)
    for row in tqdm.tqdm(data):
        row = pd.Series(list(row.values()),index=keys)
        data_to_csv = data_to_csv.append(row,ignore_index=True)
    file_name = str(location) + '#站历史曲线M' + str(month)
    print('\n导出文件：' + file_name + '\n')
    data_to_csv.to_csv(file_name +'.csv',encoding='GBK',index=False)

if __name__ == "__main__":
    ip = input('输入SQL server的ip地址： ')
    user = input('输入用户名： ')
    password = input('输入密码： ')
    print('\n----------  开始传输数据  ----------\n')
    name_dict = get_points_definition_from_server(ip,user,password)
    for location in range(1,35):
        for month in range(1,13):
            location_str = '{:0>2s}'.format(str(location))
            all_data,keys = get_data_from_server(location,month,ip,user,password)
            export_data_to_csv(all_data,keys,name_dict,location_str,location,month)
