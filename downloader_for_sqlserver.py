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
    data_to_csv = pd.DataFrame()
    count = 0
    for row in tqdm.tqdm(data):
        for column_key in keys:
            if (column_key == '年')|(column_key == '月')|(column_key == '日')|(column_key == 'SerialNumber')|(column_key == 'IDX'):
                data_to_csv.loc[count,column_key] = row[column_key]
            elif (column_key == 'AI0')|(column_key == 'AI1')|(column_key == 'AI2')|(column_key == 'AI3')|(column_key == 'AI4')|(column_key == 'AI5')| \
                (column_key == 'AI6')|(column_key == 'AI7')|(column_key == 'AI8')|(column_key == 'AI9'):
                data_to_csv.loc[count,name_dict[location_str + column_key[:2] + '0' + column_key[-1]]] = row[column_key] 
            else:
                data_to_csv.loc[count,name_dict[location_str+column_key]] = row[column_key]
        count += 1
    
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
