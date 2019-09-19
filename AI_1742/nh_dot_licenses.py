# -*- coding: utf-8 -*-

'''
Created on 2019-Sep-10 08:44:19
TICKET NUMBER -AI_1742
@author: ait-python
'''

from scrapy.loader import ItemLoader
from scrapy.loader.processors import MapCompose
from w3lib.html import remove_tags, replace_escape_chars

from Data_scuff.spiders.AI_1742.items import NhDotLicensesSpiderItem
from Data_scuff.spiders.__common import CommonSpider,CustomSettings
from Data_scuff.utils.utils import Utils

import numpy as np
# from pandas import DataFrame
import pandas as pd
import tabula
import scrapy
import tempfile
import re
import os
import sre_constants


pd.set_option('display.max_rows', 200)
pd.set_option('display.max_columns', 200)
pd.set_option('display.width', 200)


class NhDotLicensesSpider(CommonSpider):
    name = '1742_nh_dot_licenses'
    allowed_domains = ['nh.gov']
    start_urls = ['https://www.nh.gov/dot/org/administration/ofc/dbe.htm']
    
    custom_settings = {
        'FILE_NAME':Utils.getRundateFileName('AI-1742_Licenses_DOT_OR_CurationReady'),
        'JIRA_ID':'AI_1742',
        'COOKIES_ENABLED':True,
        'COOKIES_DEBUG':True,
        'HTTPCACHE_ENABLED':False,
        # 'JOBDIR' : CustomSettings.getJobDirectory('nh_dot_licenses'),
        'TOP_HEADER':{                        'additional naics code(s)': 'Additional NAICS code(s)',
                         'company_email': 'E-mail',
                         'company_fax': 'Fax',
                         'company_name': 'Name',
                         'company_phone': 'Tel',
                         'company_website': 'web site',
                         'dba_name': '',
                         'location_address_string': 'Address',
                         'naics': 'Primary NAICS Code(s):',
                         'naics_description': 'Description and/or type of work DBE is certified to perform:',
                         'permit_lic_desc': '',
                         'permit_subtype': 'TYPE',
                         'permit_type': '',
                         'person_name': 'Contact'},
        'FIELDS_TO_EXPORT':[                        
                         'company_name',
                         'dba_name',
                         'person_name',
                         'location_address_string',
                         'permit_subtype',  
                         'company_phone',
                         'company_fax',
                         'company_email',
                         'company_website',
                         'naics',
                         'additional naics code(s)',
                         'naics_description',
                         'permit_lic_desc',
                         'permit_type',
                         'sourceName',
                         'url',
                         'ingestion_timestamp'],

        'NULL_HEADERS':['additional naics code(s)']
        }

    def parse(self, response):
        df = tabula.read_pdf('https://www.nh.gov/dot/org/administration/ofc/documents/dbe-directory.pdf',
        area = [5.739,6.122,780.887,602.212],
        columns=[221.908,554.005],
        pages= 'all',
        silent = True,
        stream=True,
        guess=False,
        encoding='ISO-8859-1',
        pandas_options={'header': 'infer'}).replace('\r', ' ', regex=True).dropna(how='all').fillna('')
        li=[]
        # df.columns=['a']
        df.columns=["A","B"]
        df = df[~df['A'].str.startswith('(Includes all Transportation', na=False)]
        df = df[~df['A'].str.contains('MBE: Minority Business Enterprise WBE', na=False)]
        df = df[~df['A'].str.contains('DBE Directory - 9/6/19', na=False)]
        df = df[~df['B'].str.contains('New Hampshire Certified DBE/AC', na=False)]
        df = df[~df['A'].str.contains('New Hampshire Certified DBE/AC', na=False)]

        # print("_______df",df)
        df=df['A']+"~"+df['B']
        df=pd.DataFrame(df)
        li = []
        for _, row in df.fillna('').iterrows():
            li.extend(row)
        df[1] = df.apply(lambda x:' '.join(x.dropna().astype(str)), axis=1)
        def fillUniqueNum(v):
            if fillUniqueNum.change:
                fillUniqueNum.unique_num += 1
            fillUniqueNum.change = False
            if re.search(r'^(web site:).*',v[1]):
                fillUniqueNum.change = True
            return str(fillUniqueNum.unique_num)
        fillUniqueNum.change = False
        fillUniqueNum.unique_num = 1
        df[0] = df.apply(lambda v:fillUniqueNum(v),axis=1)
        df=df[[0,1]]
        df = df.groupby(0)[1].apply(list)
        for i,val in enumerate(df):
            if i==0:
                dff1=pd.DataFrame(val)
                dff1.columns=['A']
                split_col=dff1['A'].str.split("~",n=1,expand=True)
                dff2=pd.DataFrame(split_col[0]).fillna('')
                dff2[0] = dff2[0].replace('', np.nan)
                dff2 = dff2.dropna()
                
                li1=[]
                for _, row in dff2.fillna('').iterrows():
                    li1.extend(row)
                for i,j in enumerate(li1):
                    # print(j)
                    if j.startswith('Contact:'):
                        person_name=j.replace('Contact:','')
                    if j.startswith('Address:'):
                        location_address1=j.replace('Address:','')
                    if j.startswith('Tel:'):
                        company_phone=j.replace('Tel:','')
                    if re.search(r'[A-Z]{2}[\s][0-9]{5}',j):
                        location_address2=j
                        location_address_string=location_address1+' '+location_address2
                        # print(location_address_string)
                    if j.startswith('Fax:'):
                        company_fax=j.replace('Fax:','')
                    if j.startswith('e-mail:'):
                        company_email=j.replace('e-mail:','')
                    if j.startswith('web site:'):
                        company_website=j.replace('web site:','')
                    if ':' not in j:
                        if re.search(r'[A-Z]{2}[\s][0-9]{5}',j):
                            pass
                        else:
                            company_name1=j
                #==================================================
                dff3=pd.DataFrame(split_col[1]).fillna('')

                avc=dff3[1].tolist()
                avc='~~~'.join(avc)
                # match = re.search('Certified for work in the following areas:',str(avc))
                match = re.search('Certified for work in the following areas:(.*)Description and/or type of work DBE is certified to perform:',str(avc))
                match1 = re.search('Description and/or type of work DBE is certified to perform:(.*)',str(avc))
                match2 = re.search('(.*)Certified for work in the following areas:',str(avc))


                if match2:
                    a=match2.group()
                    b=a.split('~~~')[0]
                    print('bbbbbbbbbbbbbbbb',b)
                    if ' ' in b:
                        type1=company_name1+''.join(b)
                        type1=type1.split(' ')
                        # print('type:::::::::',type1)
                        company_name=' '.join(type1[0:-1])
                        # print('ssssss',company_name)
                        sub_type=type1[-1]
                    else:
                        company_name=company_name1
                        sub_type=b
                    if 'WBE' in sub_type:
                        sub_type='Woman Business Enterprise'
                    elif 'MWBE' in sub_type:
                        sub_type='Minority Woman Business Enterprise'
                    elif 'MBE' in sub_type:
                        sub_type='Minority Business Enterprise'
                    elif 'DBE' in sub_type:
                        sub_type='Disadvantaged Business Enterprise'
                if match1:
                    Description=match1.group()
                    desc=Description.replace('Description and/or type of work DBE is certified to perform:','').replace('~~~','')
                if match:
                    val=match.group()
                    val=val.replace('~~~~','').replace('~~','')
                    primary_naics=val.split('Primary NAICS Code(s):')[1].split('Additional NAICS code(s)')[0]
                    additional_naics=val.split('Additional NAICS code(s)')[1].split('Description and/or type of work DBE is certified to perform:')[0]

                    il = ItemLoader(item=NhDotLicensesSpiderItem(),response=response)
                    il.default_input_processor = MapCompose(lambda v: v.strip(), remove_tags, replace_escape_chars)
                    #il.add_value('ingestion_timestamp', Utils.getingestion_timestamp())
                    il.add_value('sourceName', 'NH_DOT_Licenses')
                    il.add_value('url', 'https://www.nh.gov/dot/org/administration/ofc/dbe.htm')
                    il.add_value('dba_name', '')
                    il.add_value('permit_subtype',sub_type)
                    il.add_value('naics_description',desc)
                    il.add_value('company_website',company_website)
                    il.add_value('additional naics code(s)',additional_naics)
                    il.add_value('company_name',company_name)
                    il.add_value('company_fax',company_fax)
                    il.add_value('permit_lic_desc','Transportation Licenses for'+company_name)
                    il.add_value('permit_type', 'transportation_license')
                    il.add_value('naics',primary_naics)
                    il.add_value('company_email',company_email)
                    il.add_value('company_phone',company_phone)
                    il.add_value('person_name',person_name)
                    il.add_value('location_address_string',location_address_string)
                    yield il.load_item()
            else:
                dff1=pd.DataFrame(val)
                dff1.columns=['A']
                split_col=dff1['A'].str.split("~",n=1,expand=True)
                dff2=pd.DataFrame(split_col[0]).fillna('')
                dff2[0] = dff2[0].replace('', np.nan)
                dff2 = dff2.dropna()
                
                li1=[]
                for _, row in dff2.fillna('').iterrows():
                    li1.extend(row)
                for i,j in enumerate(li1):
                    # print(j)
                    if j.startswith('Contact:'):
                        person_name=j.replace('Contact:','')
                    if j.startswith('Address:'):
                        location_address1=j.replace('Address:','')
                    if j.startswith('Tel:'):
                        company_phone=j.replace('Tel:','')
                    if re.search(r'[A-Z]{2}[\s][0-9]{5}',j):
                        location_address2=j
                        location_address_string=location_address1+', '+location_address2
                        # print(location_address_string)
                    if j.startswith('Fax:'):
                        company_fax=j.replace('Fax:','')
                    if j.startswith('e-mail:'):
                        company_email=j.replace('e-mail:','')
                    if j.startswith('web site:'):
                        company_website=j.replace('web site:','')
                    if ':' not in j:
                        if re.search(r'[A-Z]{2}[\s][0-9]{5}',j):
                            pass
                        else:
                            company_name1=j
                #==================================================
                dff3=pd.DataFrame(split_col[1]).fillna('')
                avc=dff3[1].tolist()
                avc='~~~'.join(avc)
                # match = re.search('Certified for work in the following areas:',str(avc))
                match = re.search('Certified for work in the following areas:(.*)Description and/or type of work DBE is certified to perform:',str(avc))
                match1 = re.search('Description and/or type of work DBE is certified to perform:(.*)',str(avc))
                match2 = re.search('(.*)Certified for work in the following areas:',str(avc))


                if match2:
                    a=match2.group()
                    b=a.split('~~~')[0]
                    print('bbbbbbbbbbbbbbbb',b)
                    if ' ' in b:
                        type1=company_name1+''.join(b)
                        type1=type1.split(' ')
                        # print('type:::::::::',type1)
                        company_name=' '.join(type1[0:-1])
                        # print('ssssss',company_name)
                        sub_type=type1[-1]
                    else:
                        company_name=company_name1
                        sub_type=b

                    if 'WBE' in sub_type:
                        sub_type='Woman Business Enterprise'
                    elif 'MWBE' in sub_type:
                        sub_type='Minority Woman Business Enterprise'
                    elif 'MBE' in sub_type:
                        sub_type='Minority Business Enterprise'
                    elif 'DBE' in sub_type:
                        sub_type='Disadvantaged Business Enterprise'
                if match1:
                    Description=match1.group()
                    desc=Description.replace('Description and/or type of work DBE is certified to perform:','').replace('~~~','')
                if match:
                    val=match.group()
                    val=val.replace('~~~~','').replace('~~','')
                    primary_naics=val.split('Primary NAICS Code(s):')[1].split('Additional NAICS code(s)')[0]
                    additional_naics=val.split('Additional NAICS code(s)')[1].split('Description and/or type of work DBE is certified to perform:')[0]


                    # self.state['items_count'] = self.state.get('items_count', 0) + 1
                    il = ItemLoader(item=NhDotLicensesSpiderItem(),response=response)
                    il.default_input_processor = MapCompose(lambda v: v.strip(), remove_tags, replace_escape_chars)
                    #il.add_value('ingestion_timestamp', Utils.getingestion_timestamp())
                    il.add_value('sourceName', 'NH_DOT_Licenses')
                    il.add_value('url', 'https://www.nh.gov/dot/org/administration/ofc/dbe.htm')
                    il.add_value('dba_name',self._getDBA(company_name)[1])
                    il.add_value('permit_subtype',sub_type)
                    il.add_value('naics_description',desc)
                    il.add_value('company_website',company_website)
                    il.add_value('additional naics code(s)',additional_naics.replace('NAICS CODE',''))
                    il.add_value('company_name',self._getDBA(company_name)[0])
                    il.add_value('company_fax',company_fax)
                    il.add_value('permit_lic_desc','Transportation Licenses for '+company_name)
                    il.add_value('permit_type', 'transportation_license')
                    il.add_value('naics',primary_naics.replace('NAICS CODE',''))
                    il.add_value('company_email',company_email)
                    il.add_value('company_phone',company_phone)
                    il.add_value('person_name',person_name)
                    il.add_value('location_address_string',location_address_string)
                    yield il.load_item()