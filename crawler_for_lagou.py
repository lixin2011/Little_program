import requests
import json
import pandas as pd
import time
import os
os.chdir(r'D:/temp')
# 网页链接

def get_data(keyword,page):
    '''
    input keywords and page, return a dict object
    '''
    url_1 = 'https://www.lagou.com/jobs/list_' + keyword + '/p-city_0?&cl=false&fromSearch=true&labelWords=&suginput='
    url_2 = 'https://www.lagou.com/jobs/positionAjax.json?needAddtionalResult=false'
    # 请求头
    headers = {
        "referer": "https://www.lagou.com/jobs/list_java?labelWords=&fromSearch=true&suginput=",
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/86.0.4240.75 Safari/537.36"
    }
    data = {
        'first': 'false',
        'pn': page,
        'kd': keyword
    }
    re_1 = requests.get(url=url_1, headers=headers)
    re_2 = requests.post(url=url_2, headers=headers, data=data, cookies=re_1.cookies)
    data = json.loads(re_2.text)
    print(data)
    return data


def resolve_data(data):
    '''
    return a DataFrame
    '''
    results = pd.DataFrame()
    for i in range(15):
        results.loc[i,'职位'] = data['content']['positionResult']['result'][i]['positionName']
        results.loc[i,'公司简称'] = data['content']['positionResult']['result'][i]['companyShortName']
        results.loc[i,'公司全称'] = data['content']['positionResult']['result'][i]['companyFullName']
        results.loc[i,'公司规模'] = data['content']['positionResult']['result'][i]['companySize']
        results.loc[i,'业务范围'] = data['content']['positionResult']['result'][i]['industryField']
        results.loc[i,'城市'] = data['content']['positionResult']['result'][i]['city']
        results.loc[i,'地区'] = data['content']['positionResult']['result'][i]['district']
        results.loc[i,'薪酬'] = data['content']['positionResult']['result'][i]['salary']
        results.loc[i,'创建时间'] = data['content']['positionResult']['result'][i]['createTime']
        print(results)

    return results

if __name__ == "__main__":
    results = pd.DataFrame(columns=['职位','公司简称','公司全称','公司规模','业务范围','城市','地区','薪酬','创建时间'])
    keyword = input('输入查询的关键字：')
    start_page = int(input('输入查询的开始页数：'))
    end_page = int(input('输入查询的终止页数：'))
    for page in range(start_page,end_page + 1):
        data = get_data(keyword,page)
        result = resolve_data(data)
        results = pd.concat([results,result])
        try:
            results.to_csv('lagou_'+keyword+'_'+str(start_page)+'_'+str(end_page)+'.csv',index=False,encoding='gb18030')
        except UnicodeEncodeError as e:
            print(e)
        time.sleep(20)
    print(results)