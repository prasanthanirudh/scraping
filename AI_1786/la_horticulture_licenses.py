# -*- coding: utf-8 -*-

'''
Created on 2019-Sep-18 01:46:00
TICKET NUMBER -AI_1786
@author: ait-python
'''

from scrapy.loader import ItemLoader
from scrapy.loader.processors import MapCompose
from w3lib.html import remove_tags, replace_escape_chars

from Data_scuff.spiders.AI_1786.items import LaHorticultureLicensesSpiderItem
from Data_scuff.spiders.__common import CommonSpider,CustomSettings
from Data_scuff.utils.utils import Utils

import urllib.request


import tabula
import pandas as pd
import re
import numpy as np
import scrapy
import tempfile
import os


class LaHorticultureLicensesSpider(CommonSpider):
    name = '1786_la_horticulture_licenses'
    allowed_domains = ['state.la.us']
    start_urls = ['http://www.ldaf.state.la.us/ldaf-programs/horticulture-programs/louisiana-horticulture-commission/']
    
    custom_settings = {
        'FILE_NAME':Utils.getRundateFileName('AI-1786_Licenses_Horticulture_LA_CurationReady_'),
        'JIRA_ID':'AI_1786',
        'HTTPCACHE_ENABLED':False,
        'COOKIES_ENABLED':True,
        'COOKIES_DEBUG':True,
        # 'JOBDIR' : CustomSettings.getJobDirectory('la_horticulture_licenses'),
        'TOP_HEADER':{                        'address': 'Address',
                         'city': 'City',
                         'company_name': 'Name',
                         'company_phone': 'Buss Phone',
                         'dba_name': '',
                         'location_address_string': '',
                         'parish': 'Parish',
                         'permit_lic_desc': '',
                         'permit_lic_no': 'PermitNo',
                         'permit_subtype': 'Professionals List',
                         'permit_type': '',
                         'permitcode': 'PermitCode',
                         'person_name': 'ContactInfo',
                         'place of business': 'Place of Business',
                         'specialties': 'Specialties',
                         'st': 'St',
                         'zip': 'Zip'},
        'FIELDS_TO_EXPORT':[                        
                         'permit_subtype',
                         'parish',
                         'permit_lic_no',
                         'company_name',
                         'dba_name',
                         'person_name',
                         'company_phone',
                         'place of business',
                         'location_address_string',
                         'address',
                         'city',
                         'st',
                         'zip',
                         'permitcode',
                         'specialties',
                         'permit_lic_desc',
                         'permit_type',
                         'sourceName',
                         'url',
                         'ingestion_timestamp',

                         ],
        'NULL_HEADERS':['permitcode', 'st', 'specialties', 'parish', 'city', 'place of business', 'address', 'zip']
        }

    def parse(self, response):
        meta={}
        meta['parish']=meta['permit_subtype']=meta['company_phone']=meta['company_name']=meta['location_address']=meta['place_of_bussiness']=meta['city']=meta['state']=meta['zip_code']=meta['permit_lic_no']=meta['person_name']=meta['permit_lic_no']=meta['permitcode']=meta['specialties']=''

        pdf_link=response.xpath('//*[@id="mainCol"]/ul[1]/li/a/@href').extract()
        for i in pdf_link:
            print("---------------",i)
            if i == 'http://www.ldaf.state.la.us/wp-content/uploads/2019/09/ARL.pdf':
                class AppURLopener(urllib.request.FancyURLopener):
                    version = "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/75.0.3770.100 Safari/537.36"

                opener = AppURLopener()
                file = opener.open('http://www.ldaf.state.la.us/wp-content/uploads/2019/09/ARL.pdf')        
                df = tabula.read_pdf(file,
                pages= 'all',
                guess=False,
                area = [16.335,14.85,587.565,775.17],
                columns=[83.16,246.51,302.94,612.81,700.92,721.71,774.18],
                encoding='ISO-8859-1',
                pandas_options={'header': None}).replace('\r', ' ', regex=True).dropna(how='all').fillna('')
                df.columns=['a','b','c','d','e','f','g']
                end_index = df[df['b']=='Name'].index.tolist()
                index_list_2 = [i for i in range(df.index[0],end_index[0])]
                df.drop(df.index[index_list_2], inplace=True)
                df = df[~df['b'].str.contains('Name', na=False)]
                for _, row in df.iterrows():
                    a=row.tolist()
                    meta['parish']=a[0]
                    meta['permit_subtype']='Arborist'
                    meta['company_name']=a[1]
                    meta['company_phone']=a[2]
                    meta['place_of_bussiness']=a[3]
                    meta['city']=a[4]
                    meta['state']=a[5]
                    meta['zip_code']=a[6]
                    meta['location_address_string']=self.format__address_3(meta['city'],meta['state'],meta['zip_code'])
                    yield self.save_to_csv(response,**meta)



            if i=='http://www.ldaf.state.la.us/wp-content/uploads/2019/09/UA.pdf':
                class AppURLopener(urllib.request.FancyURLopener):
                    version = "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/75.0.3770.100 Safari/537.36"

                opener = AppURLopener()
                file = opener.open('http://www.ldaf.state.la.us/wp-content/uploads/2019/09/UA.pdf') 
                df = tabula.read_pdf(file,
                pages= 'all',
                guess=False,
                area = [16.335,14.85,598.455,771.21],
                columns=[97.02,241.56,300,595.98,693.0,715.77,766.26],
                encoding='ISO-8859-1',
                pandas_options={'header': None}).replace('\r', ' ', regex=True).dropna(how='all').fillna('')
                df.columns=['a','b','c','d','e','f','g']
                end_index = df[df['b']=='Name'].index.tolist()
                index_list_2 = [i for i in range(df.index[0],end_index[0])]
                df.drop(df.index[index_list_2], inplace=True)
                df = df[~df['b'].str.contains('Name', na=False)]
                for _, row in df.iterrows():
                    a=row.tolist()
                    meta['parish']=a[0]
                    meta['permit_subtype']='UTILITY ARBORIST'
                    meta['company_name']=a[1]
                    meta['company_phone']=a[2]
                    meta['place_of_bussiness']=a[3]
                    meta['city']=a[4]
                    meta['state']=a[5]
                    meta['zip_code']=a[6]
                    meta['location_address_string']=self.format__address_3(meta['city'],meta['state'],meta['zip_code'])
                    yield self.save_to_csv(response,**meta)

            if i=='http://www.ldaf.state.la.us/wp-content/uploads/2019/09/LA.pdf':
                class AppURLopener(urllib.request.FancyURLopener):
                    version = "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/75.0.3770.100 Safari/537.36"

                opener = AppURLopener()
                file = opener.open('http://www.ldaf.state.la.us/wp-content/uploads/2019/09/LA.pdf') 
                df = tabula.read_pdf(file,
                pages= 'all',
                guess=False,
                area = [16.335,14.85,598.455,771.21],
                columns=[97.02,241.56,320,595.98,693.0,715.77,766.26],
                encoding='ISO-8859-1',
                pandas_options={'header': None}).replace('\r', ' ', regex=True).dropna(how='all').fillna('')
                df.columns=['a','b','c','d','e','f','g']
                end_index = df[df['b']=='Name'].index.tolist()
                index_list_2 = [i for i in range(df.index[0],end_index[0])]
                df.drop(df.index[index_list_2], inplace=True)
                df = df[~df['b'].str.contains('Name', na=False)]
                for _, row in df.iterrows():
                    a=row.tolist()
                    meta['parish']=a[0]
                    meta['permit_subtype']='LANDSCAPE ARCHITECT'
                    meta['company_name']=a[1]
                    meta['company_phone']=a[2]
                    meta['place_of_bussiness']=a[3]
                    meta['city']=a[4]
                    meta['state']=a[5]
                    meta['zip_code']=a[6]
                    meta['location_address_string']=self.format__address_3(meta['city'],meta['state'],meta['zip_code'])
                    yield self.save_to_csv(response,**meta)

            if i=='http://www.ldaf.state.la.us/wp-content/uploads/2019/09/IC.pdf':
                class AppURLopener(urllib.request.FancyURLopener):
                    version = "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/75.0.3770.100 Safari/537.36"

                opener = AppURLopener()
                file = opener.open('http://www.ldaf.state.la.us/wp-content/uploads/2019/09/IC.pdf')
                
                df = tabula.read_pdf(file,
                pages= 'all',
                guess=False,
                area = [16.335,14.85,598.455,771.21],
                columns=[97.02,241.56,330,595.98,693.0,715.77,766.26],
                encoding='ISO-8859-1',
                pandas_options={'header': None}).replace('\r', ' ', regex=True).dropna(how='all').fillna('')
                df.columns=['a','b','c','d','e','f','g']
                end_index = df[df['b']=='Name'].index.tolist()
                index_list_2 = [i for i in range(df.index[0],end_index[0])]
                df.drop(df.index[index_list_2], inplace=True)
                df = df[~df['b'].str.contains('Name', na=False)]
                for _, row in df.iterrows():
                    a=row.tolist()
                    meta['parish']=a[0]
                    meta['permit_subtype']='IRRIGATION CONTRACTOR'
                    meta['company_name']=a[1]
                    meta['company_phone']=a[2]
                    meta['place_of_bussiness']=a[3]
                    meta['city']=a[4]
                    meta['state']=a[5]
                    meta['zip_code']=a[6]
                    meta['location_address_string']=self.format__address_3(meta['city'],meta['state'],meta['zip_code'])
                    yield self.save_to_csv(response,**meta)



            if i=='http://www.ldaf.state.la.us/wp-content/uploads/2019/09/RF.pdf':
                class AppURLopener(urllib.request.FancyURLopener):
                    version = "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/75.0.3770.100 Safari/537.36"

                opener = AppURLopener()
                file = opener.open('http://www.ldaf.state.la.us/wp-content/uploads/2019/09/RF.pdf')
                df = tabula.read_pdf(file,
                pages= 'all',
                guess=False,
                area = [16.335,14.85,598.455,771.21],
                columns=[97.02,241.56,320,595.98,693.0,715.77,766.26],
                encoding='ISO-8859-1',
                pandas_options={'header': None}).replace('\r', ' ', regex=True).dropna(how='all').fillna('')
                df.columns=['a','b','c','d','e','f','g']
                end_index = df[df['b']=='Name'].index.tolist()
                index_list_2 = [i for i in range(df.index[0],end_index[0])]
                df.drop(df.index[index_list_2], inplace=True)
                df = df[~df['b'].str.contains('Name', na=False)]
                for _, row in df.iterrows():
                    a=row.tolist()
                    meta['parish']=a[0]
                    meta['permit_subtype']='RETAIL FLORIST'
                    meta['company_name']=a[1]
                    meta['company_phone']=a[2]
                    meta['place_of_bussiness']=a[3]
                    meta['city']=a[4]
                    meta['state']=a[5]
                    meta['zip_code']=a[6]
                    meta['location_address_string']=self.format__address_3(meta['city'],meta['state'],meta['zip_code'])
                    yield self.save_to_csv(response,**meta)

            if i=='http://www.ldaf.state.la.us/wp-content/uploads/2019/09/Cut-Flower-Dealer-Permit.pdf':
                class AppURLopener(urllib.request.FancyURLopener):
                    version = "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/75.0.3770.100 Safari/537.36"

                opener = AppURLopener()
                file = opener.open('http://www.ldaf.state.la.us/wp-content/uploads/2019/09/Cut-Flower-Dealer-Permit.pdf')
                df = tabula.read_pdf(file,
                pages= 'all',
                guess=False,
                area = [34.155,14.85,557.865,735.57],
                columns=[79.2,113.85,277.2,432.63,569.25,628.65,665.28,689.04,736.56],
                encoding='ISO-8859-1',
                pandas_options={'header': None}).replace('\r', ' ', regex=True).dropna(how='all').fillna('')
                df.columns=['a','b','c','d','e','f','g','h','i']
                end_index = df[df['a']=='Parish'].index.tolist()
                index_list_2 = [i for i in range(df.index[0],end_index[0])]
                df.drop(df.index[index_list_2], inplace=True)
                df = df[~df['a'].str.contains('Parish', na=False)]
                for _, row in df.iterrows():
                    a=row.tolist()
                    meta['parish']=a[0]
                    meta['permit_lic_no']=a[1]
                    meta['permit_subtype']='CUT FLOWER DEALER PERMIT'
                    meta['company_name']=a[2]
                    meta['person_name']=a[3]
                    meta['location_address']=a[4]
                    meta['city']=a[5]
                    meta['state']=a[6]
                    meta['zip_code']=a[7]
                    meta['company_phone']=a[8]
                    meta['location_address_string']=self.format__address_3(meta['city'],meta['state'],meta['zip_code'])
                    yield self.save_to_csv(response,**meta)

            if i=='http://www.ldaf.state.la.us/wp-content/uploads/2019/09/WF.pdf':
                class AppURLopener(urllib.request.FancyURLopener):
                    version = "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/75.0.3770.100 Safari/537.36"

                opener = AppURLopener()
                file = opener.open('http://www.ldaf.state.la.us/wp-content/uploads/2019/09/WF.pdf')
                df = tabula.read_pdf(file,
                pages= 'all',
                guess=False,
                area = [12.375,15.84,600.435,782.1],
                columns=[133.65,300.96,382.14,594.99,680.13,719.73,782.1],
                encoding='ISO-8859-1',
                pandas_options={'header': None}).replace('\r', ' ', regex=True).dropna(how='all').fillna('')
                df.columns=['a','b','c','d','e','f','g']
                end_index = df[df['a']=='Parish'].index.tolist()
                index_list_2 = [i for i in range(df.index[0],end_index[0])]
                df.drop(df.index[index_list_2], inplace=True)
                df = df[~df['a'].str.contains('Parish', na=False)]
                print(df)
                for _, row in df.iterrows():
                    a=row.tolist()
                    meta['parish']=a[0]
                    meta['permit_subtype']='WHOLESALE FLORIST'
                    meta['company_name']=a[1]
                    meta['company_phone']=a[2]
                    meta['place_of_bussiness']=a[3]
                    meta['city']=a[4]
                    meta['state']=a[5]
                    meta['zip_code']=a[6]
                    meta['location_address_string']=self.format__address_3(meta['city'],meta['state'],meta['zip_code'])
                    yield self.save_to_csv(response,**meta)

            if i=='http://www.ldaf.state.la.us/wp-content/uploads/2019/09/Nursery-Certificate-NSD12.pdf':
                class AppURLopener(urllib.request.FancyURLopener):
                    version = "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/75.0.3770.100 Safari/537.36"

                opener = AppURLopener()
                file = opener.open('http://www.ldaf.state.la.us/wp-content/uploads/2019/09/Nursery-Certificate-NSD12.pdf')
                df = tabula.read_pdf(file,
                pages= 'all',
                guess=False,
                area = [33.165,11.88,597.465,773.19],
                columns=[26.73,72.27,98.01,215.82,315.81,422.73,458.37,492.03,517.77,550.44,567.27,674.19],
                encoding='ISO-8859-1',
                pandas_options={'header': 'infer'}).replace('\r', ' ', regex=True).dropna(how='all').fillna('')
                df.columns=['a','b','c','d','e','f','g','h','i','j','k','l','m']
                for _, row in df.iterrows():
                    a=row.tolist()
                    meta['permit_subtype']='Nursery Certificate'
                    meta['parish']=a[1]
                    meta['permit_lic_no']=a[2]
                    meta['company_name']=a[3]
                    meta['person_name']=a[4]
                    meta['company_phone']=a[9]
                    meta['permitcode']=a[10]
                    meta['specialties']=a[11]
                    loc=a[5]
                    meta['city']=a[6]
                    meta['state']=a[7]
                    meta['zip_code']=a[8]
                    meta['location_address_string']=self.format__address_4(loc,meta['city'],meta['state'],meta['zip_code'])
                    yield self.save_to_csv(response,**meta)

            if i=='http://www.ldaf.state.la.us/wp-content/uploads/2019/09/LH.pdf':
                class AppURLopener(urllib.request.FancyURLopener):
                    version = "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/75.0.3770.100 Safari/537.36"

                opener = AppURLopener()
                file = opener.open('http://www.ldaf.state.la.us/wp-content/uploads/2019/09/LH.pdf')
                df = tabula.read_pdf(file,
                pages= 'all',
                guess=False,
                area = [16.335,14.85,598.455,771.21],
                columns=[97.02,241.56,310,595.98,693.0,710,766.26],
                encoding='ISO-8859-1',
                pandas_options={'header': None}).replace('\r', ' ', regex=True).dropna(how='all').fillna('')
                df.columns=['a','b','c','d','e','f','g']
                end_index = df[df['b']=='Name'].index.tolist()
                index_list_2 = [i for i in range(df.index[0],end_index[0])]
                df.drop(df.index[index_list_2], inplace=True)
                df = df[~df['b'].str.contains('Name', na=False)]
                for _, row in df.iterrows():
                    a=row.tolist()
                    meta['parish']=a[0]
                    meta['permit_subtype']='LANDSCAPE HORTICULTURIST'
                    meta['company_name']=a[1]
                    meta['company_phone']=a[2]
                    meta['place_of_bussiness']=a[3]
                    meta['city']=a[4]
                    meta['state']=a[5]
                    meta['zip_code']=a[6]
                    meta['location_address_string']=self.format__address_3(meta['city'],meta['state'],meta['zip_code'])
                    yield self.save_to_csv(response,**meta)

            if i=='http://www.ldaf.state.la.us/wp-content/uploads/2019/09/Nursery-Stock-Dealer-Permit.pdf':
                class AppURLopener(urllib.request.FancyURLopener):
                    version = "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/75.0.3770.100 Safari/537.36"

                opener = AppURLopener()
                file = opener.open('http://www.ldaf.state.la.us/wp-content/uploads/2019/09/Nursery-Stock-Dealer-Permit.pdf')
                df = tabula.read_pdf(file,
                pages= '1-13',
                guess=False,
                area = [35.145,13.86,559.845,764.28],
                columns=[83.16,109.89,291.06,450.45,594.99,662.31,690.03,720,760.32],
                encoding='ISO-8859-1',
                pandas_options={'header': None}).replace('\r', ' ', regex=True).dropna(how='all').fillna('')
                df.columns=['a','b','c','d','e','f','g','h','i']
                end_index = df[df['a']=='Parish'].index.tolist()
                index_list_2 = [i for i in range(df.index[0],end_index[0])]
                df.drop(df.index[index_list_2], inplace=True)
                for _, row in df.iterrows():
                    a=row.tolist()
                    meta['parish']=a[0]
                    meta['permit_lic_no']=a[1]
                    meta['permit_subtype']='NURSERY STOCK DEALER PERMIT'
                    meta['company_name']=a[2]
                    meta['person_name']=a[3]
                    meta['location_address']=a[4]
                    meta['city']=a[5]
                    meta['state']=a[6]
                    meta['zip_code']=a[7]
                    meta['company_phone']=a[8]
                    meta['location_address_string']=self.format__address_3(meta['city'],meta['state'],meta['zip_code'])
                    yield self.save_to_csv(response,**meta)

            if i=='http://www.ldaf.state.la.us/wp-content/uploads/2019/09/Nursery-Stock-Dealer-Permit.pdf':
                class AppURLopener(urllib.request.FancyURLopener):
                    version = "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/75.0.3770.100 Safari/537.36"

                opener = AppURLopener()
                file = opener.open('http://www.ldaf.state.la.us/wp-content/uploads/2019/09/Nursery-Stock-Dealer-Permit.pdf')
                df = tabula.read_pdf(file,
                pages='14-25',
                guess=False,
                area = [27.225,11.88,566.775,777.15],
                columns=[59.4,109.89,289.08,436.59,590.04,657.36,681.12,721.71,770.22],
                encoding='ISO-8859-1',
                pandas_options={'header': None}).replace('\r', ' ', regex=True).dropna(how='all').fillna('')
                df.columns=['a','b','c','d','e','f','g','h','i']
                for _, row in df.iterrows():
                    a=row.tolist()
                    meta['parish']=a[0]
                    meta['permit_lic_no']=a[1]
                    meta['permit_subtype']='NURSERY STOCK DEALER PERMIT'
                    meta['company_name']=a[2]
                    meta['person_name']=a[3]
                    meta['location_address']=a[4]
                    meta['city']=a[5]
                    meta['state']=a[6]
                    meta['zip_code']=a[7]
                    meta['company_phone']=a[8]
                    meta['location_address_string']=self.format__address_3(meta['city'],meta['state'],meta['zip_code'])
                    yield self.save_to_csv(response,**meta)


            


    def save_to_csv(self,response,**meta):
        # self.state['items_count'] = self.state.get('items_count', 0) + 1
        il = ItemLoader(item=LaHorticultureLicensesSpiderItem(),response=response)
        # il.default_input_processor = MapCompose(lambda v: v.strip(), remove_tags, replace_escape_chars)
        # #il.add_value('ingestion_timestamp', Utils.getingestion_timestamp())
        il.add_value('url', 'http://www.ldaf.state.la.us/ldaf-programs/horticulture-programs/louisiana-horticulture-commission/')
        il.add_value('sourceName', 'LA_Horticulture_Licenses')
        il.add_value('permit_lic_desc','Specialty Business License for '+meta['company_name'])
        il.add_value('permitcode',meta['permitcode'])
        il.add_value('address',meta['location_address'])
        il.add_value('company_name', self._getDBA(meta['company_name'])[0])
        il.add_value('city',meta['city'])
        il.add_value('zip', meta['zip_code'])
        il.add_value('person_name',meta['person_name'])
        il.add_value('dba_name',self._getDBA(meta['company_name'])[1])
        il.add_value('st', meta['state'])
        il.add_value('permit_type', 'specialty_business_license')
        il.add_value('parish',meta['parish'])
        il.add_value('place of business',meta['place_of_bussiness'])
        il.add_value('permit_lic_no',meta['permit_lic_no'])
        il.add_value('location_address_string',meta['location_address_string'] if meta['location_address_string'] else 'LA')
        il.add_value('company_phone',meta['company_phone'])
        il.add_value('specialties',meta['specialties'])
        il.add_value('permit_subtype',meta['permit_subtype'])
        return il.load_item()