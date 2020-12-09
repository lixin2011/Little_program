import requests
from lxml import html
import re
import pandas as pd
import time
import os
os.chdir(r'D:/temp')
# 网页链接
def get_data(page,type):
    '''
    输入页数，返回一个网页的响应对象
    '''
    url = 'https://jobs.51job.com/'+type+'/p'+str(page)+'/'
    # 请求头
    headers = {
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9",
        "Accept-Encoding": "gzip, deflate, br",
        "Accept-Language": "zh-CN,zh;q=0.9",
        "Connection": "keep-alive",
        "Cookie": "guid=7e8a970a750a4e74ce237e74ba72856b; partner=blog_csdn_net",
        "Host": "jobs.51job.com",
        "Sec-Fetch-Dest": "document",
        "Sec-Fetch-Mode": "navigate",
        "Sec-Fetch-Site": "none",
        "Sec-Fetch-User": "?1",
        "Upgrade-Insecure-Requests": "1",
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/86.0.4240.75 Safari/537.36"
    }
    # 有请求头写法
    res = requests.get(url=url, headers=headers)
    return res

def locate_data(res,id,datatype):
    '''
    输入响应对象，返回各个类型数据对应的列表
    '''
    re_dict = {
    'job':['>.*</a>$',-4],
    'company':['>.*</a>$',-4],
    'region':['>.*</span>$',-7],
    'salary':['>.*</span>$',-7],
    'day':['>.*</span>$',-7]
    }
    path_dict = {
    'job':'/html/body/div[4]/div[2]/div[1]/div[2]/div[' + str(id) + ']/p[1]/span[1]/a',
    'company':'/html/body/div[4]/div[2]/div[1]/div[2]/div[' + str(id) + ']/p[1]/a',
    'region':'/html/body/div[4]/div[2]/div[1]/div[2]/div[' + str(id) + ']/p[1]/span[2]',
    'salary':'/html/body/div[4]/div[2]/div[1]/div[2]/div[' + str(id) + ']/p[1]/span[3]',
    'day':'/html/body/div[4]/div[2]/div[1]/div[2]/div[' + str(id) + ']/p[1]/span[4]'
    }
    res.encoding = 'gbk'
    html_elements = html.etree.HTML(res.text)
    # 使用了html.etree模块的HTML方法来解析一个网页源代码，获得一个element对象
    result_element = html_elements.xpath(path_dict[datatype])[0]
    # 此处返回的是只有一个element元素的列表
    try:
        result_str = html.etree.tostring(result_element,encoding='utf-8').decode('utf-8')
        result = re.search(re_dict[datatype][0],result_str)
        result_filltration = result.group(0)[1:re_dict[datatype][1]]
        print(result_filltration)
        return result_filltration
    except AttributeError:
        print('数据未找到')

def main(type,end_page):
    
    job_list = []
    company_list = []
    region_list = []
    salary_list = []
    day_list = []
    for page in range(1,end_page):
        time.sleep(4)
        res = get_data(page,type)
        for id in range(1,21):
            job_list.append(locate_data(res,id,'job'))
            company_list.append(locate_data(res,id,'company'))
            region_list.append(locate_data(res,id,'region'))
            salary_list.append(locate_data(res,id,'salary'))
            day_list.append(locate_data(res,id,'day'))
    print(job_list)
    df_result = (pd.DataFrame([job_list,company_list,region_list,salary_list,day_list],index=['job','company','region','salary','day'])).T
    df_result.to_csv(type+'.csv',encoding='gbk',index=False)
    print(df_result)

if __name__ == "__main__":
    type = input('输入查询类别: ')
    end_page =  int(input('输入终止页数: '))
    main(type,end_page)
        


