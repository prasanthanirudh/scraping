# -*- coding: utf-8 -*-

'''
Created on 2019-Aug-08 07:07:54
TICKET NUMBER -AI_1573
@author: ait-python
'''

from scrapy.loader import ItemLoader
from scrapy.loader.processors import MapCompose
from w3lib.html import remove_tags, replace_escape_chars

from Data_scuff.spiders.AI_1573.items import OkAlcoholLicensesSpiderItem
from Data_scuff.spiders.__common import CommonSpider,CustomSettings
from Data_scuff.utils.utils import Utils
import scrapy
import tabula
import pandas as pd
import re
import numpy as np
from PyPDF2 import PdfFileReader

pd.set_option('display.max_rows', 200)
pd.set_option('display.max_columns', 200)
pd.set_option('display.width', 200)


class OkAlcoholLicensesSpider(CommonSpider):

    name = '1573_ok_alcohol_licenses'
    allowed_domains = ['ok.gov']
    start_urls = ['https://www.ok.gov/able/Monthly__Reports/index.html']
    
    custom_settings = {
        'FILE_NAME':Utils.getRundateFileName('AI-1573_Licenses_Alcohol_OK_CurationReady'),
        'JIRA_ID':'AI_1573',
        'HTTPCACHE_ENABLED': False,
        'COOKIES_ENABLED':True,
        'COOKIES_DEBUG':True,
        # 'JOBDIR' : CustomSettings.getJobDirectory('ok_alcohol_licenses'),
        'TOP_HEADER':{                        
                         'company_name': 'Licensee Name',
                         'company_phone': 'Phone number',
                         'county': 'County',
                         'dba_name': 'DBA NAME',
                         'location_address_string': 'Address',
                         'permit_lic_exp_date': 'Expires',
                         'permit_lic_no': 'Licensee number',
                         'permit_subtype': 'Type',
                         'permit_type': ''},
        'FIELDS_TO_EXPORT':[
            'permit_subtype', 
            'company_name',
            'dba_name', 
            'permit_lic_no', 
            'location_address_string',
            'city',
            'state',
            'zip_code', 
            'county', 
            'company_phone', 
            'permit_lic_exp_date', 
            'permit_type', 
            'url', 
            'ingestion_timestamp', 
            'sourceName'], 
        'NULL_HEADERS':['county']
        }


    def parse(self, response):
       
        main_url='https://www.ok.gov'
        name=response.xpath("//tr[1]/td[2]/div/div[@class='body_copy']/div[@class='cms_editor_content']/p[@class='fineprint']//a/text()").extract()[:-5]
        href_link=response.xpath("//tr[1]/td[2]/div/div[@class='body_copy']/div[@class='cms_editor_content']/p[@class='fineprint']//a/@href").extract()[:-5]
        for n,h in zip(name,href_link):
            permit_subtype=n
            pdf_link=main_url+h
            yield scrapy.Request(url=pdf_link, callback = self.parse_pdf, dont_filter=True,meta={'permit_subtype':permit_subtype})
            

    def __extractData(self,response):

        def rolling_group(val):
            if pd.notnull(str(val)) and str(val)!="True":
                rolling_group.group += 1
            return rolling_group.group
        rolling_group.group = 0  

                
        def joinFunc(g, column):
            col = g[column]
            joiner = "/"
            s = joiner.join([str(each) for each in col if pd.notnull(each)])
            s = re.sub("(?<=&)" + joiner, " ", s)  
            s = re.sub("(?<=-)" + joiner, " ", s)  
            s = re.sub(joiner * 2, joiner, s)
            return s

        pdf_link=response.url
        def getDf(pdf_link,area,columns):
            return tabula.read_pdf(pdf_link,
                pages='all',
                encoding='ISO-8859-1',
                area=area,
                columns =columns,
                guess=False,
                pandas_options={'header':None, "error_bad_lines":False, "warn_bad_lines":False})

        if str(response.url)=='https://www.ok.gov/able/documents/JULY%2C%202019%20%28BAW%29.pdf':
            df = getDf(pdf_link,[89.595,27.72,572.715,760.32],[75,99.9,158.4,278.19,410,536.58,605.88,623.7,653.4,714.78,760.32])
            df.columns = ['license_no', 'type', 'county', 'dba_name', 'license_name', 'address', 'city', 'state','zip_code', 'company_phone', 'permit_lic_exp_date']
            df['license_no'] = df['license_no'].fillna(method='bfill')
            df['boolean'] = df.license_no.eq(df.license_no.shift())
            df['license_no'] = df.apply(lambda x:x['boolean'] if not x[11]==False else x[0], axis = 1)
            df = df.drop(['boolean'], axis=1)
            groups = df.groupby(df['license_no'].apply(rolling_group), as_index=False)
            groupFunct = lambda g: pd.Series([joinFunc(g, col) for col in g.columns], index=g.columns)
            df['license_no'] = df['license_no'].replace(True, '')
            final_df = groups.apply(groupFunct).fillna('')
            return final_df.to_dict('records')

        if str(response.url)=='https://www.ok.gov/able/documents/JULY%2C%202019%20%28BED%29.pdf':
            try:
                df = getDf(pdf_link,[88.605,33.165,582.615,749.925],[73.755,102.465,165.825,272.745,404.415,511.335,590.535,607.365,637.065,702.405,746.955])
                df.columns = ['license_no', 'type', 'county', 'dba_name', 'license_name', 'address', 'city', 'state','zip_code', 'company_phone', 'permit_lic_exp_date']
                df['license_no'] = df['license_no'].fillna(method='bfill')
                df['boolean'] = df.license_no.eq(df.license_no.shift())
                df['license_no'] = df.apply(lambda x:x['boolean'] if not x[11]==False else x[0], axis = 1)
                df = df.drop(['boolean'], axis=1)
                groups = df.groupby(df['license_no'].apply(rolling_group), as_index=False)
                groupFunct = lambda g: pd.Series([joinFunc(g, col) for col in g.columns], index=g.columns)
                df['license_no'] = df['license_no'].replace(True, '')
                final_df = groups.apply(groupFunct).fillna('')
                return final_df.to_dict('records')
            except:
                pass

        if str(response.url)=='https://www.ok.gov/able/documents/JULY%2C%202019%20%28BWH%29.pdf':
            try:
                df = getDf(pdf_link,[90.585,29.205,586.575,745.965],[70.785,99.495,160.875,258.885,352.935,471.735,551.925,575,632.115,697.455,745.965])
                df.columns = ['license_no', 'type', 'county', 'dba_name', 'license_name', 'address', 'city', 'state','zip_code', 'company_phone', 'permit_lic_exp_date']
                df['license_no'] = df['license_no'].fillna(method='bfill')
                df['boolean'] = df.license_no.eq(df.license_no.shift())
                df['license_no'] = df.apply(lambda x:x['boolean'] if not x[11]==False else x[0], axis = 1)
                df = df.drop(['boolean'], axis=1)
                groups = df.groupby(df['license_no'].apply(rolling_group), as_index=False)
                groupFunct = lambda g: pd.Series([joinFunc(g, col) for col in g.columns], index=g.columns)
                df['license_no'] = df['license_no'].replace(True, '')
                final_df = groups.apply(groupFunct).fillna('')
                return final_df.to_dict('records')
            except:
                pass

        if str(response.url)=='https://www.ok.gov/able/documents/JULY%2C%202019%20%28BRP%29.pdf':
            try:
                df = getDf(pdf_link,[90.585,29.205,586.575,745.965],[73.755,102.465,165.825,272.745,404.415,511.335,590.535,610,640,690,745.955])
                df.columns = ['license_no', 'type', 'county', 'dba_name', 'license_name', 'address', 'city', 'state','zip_code', 'company_phone', 'permit_lic_exp_date']
                
                df['license_no'] = df['license_no'].fillna(method='bfill')
                df['boolean'] = df.license_no.eq(df.license_no.shift())
                df['license_no'] = df.apply(lambda x:x['boolean'] if not x[11]==False else x[0], axis = 1)
                df = df.drop(['boolean'], axis=1)
                groups = df.groupby(df['license_no'].apply(rolling_group), as_index=False)
                groupFunct = lambda g: pd.Series([joinFunc(g, col) for col in g.columns], index=g.columns)
                df['license_no'] = df['license_no'].replace(True, '')
                final_df = groups.apply(groupFunct).fillna('')
                return final_df.to_dict('records')
            except:
                pass

        if str(response.url)=='https://www.ok.gov/able/documents/JULY%2C%202019%20%28CAT%29.pdf':
            try:
                df = getDf(pdf_link,[86.625,22.275,577.665,751.905],[73.755,102.465,165.825,272.745,385,505,590,605,630,690,745.955])
                df.columns = ['license_no', 'type', 'county', 'dba_name', 'license_name', 'address', 'city', 'state','zip_code', 'company_phone', 'permit_lic_exp_date']
                
                df['license_no'] = df['license_no'].fillna(method='bfill')
                df['boolean'] = df.license_no.eq(df.license_no.shift())
                df['license_no'] = df.apply(lambda x:x['boolean'] if not x[11]==False else x[0], axis = 1)
                df = df.drop(['boolean'], axis=1)
                groups = df.groupby(df['license_no'].apply(rolling_group), as_index=False)
                groupFunct = lambda g: pd.Series([joinFunc(g, col) for col in g.columns], index=g.columns)
                df['license_no'] = df['license_no'].replace(True, '')
                final_df = groups.apply(groupFunct).fillna('')
                return final_df.to_dict('records')
            except:
                pass

        if str(response.url)=='https://www.ok.gov/able/documents/JULY%2C%202019%20%28DSP%29.pdf':
            try:
                df = getDf(pdf_link,[91.575,25.74,571.725,748.44],[63.36,100.98,162.36,261.36,366.3,491.04,578.16,605.88,633.6,692.01,748.44])
                df.columns = ['license_no', 'type', 'county', 'dba_name', 'license_name', 'address', 'city', 'state','zip_code', 'company_phone', 'permit_lic_exp_date']
                df['license_no'] = df['license_no'].fillna(method='bfill')
                df['boolean'] = df.license_no.eq(df.license_no.shift())
                df['license_no'] = df.apply(lambda x:x['boolean'] if not x[11]==False else x[0], axis = 1)
                df = df.drop(['boolean'], axis=1)
                groups = df.groupby(df['license_no'].apply(rolling_group), as_index=False)
                groupFunct = lambda g: pd.Series([joinFunc(g, col) for col in g.columns], index=g.columns)
                df['license_no'] = df['license_no'].replace(True, '')
                final_df = groups.apply(groupFunct).fillna('')
                return final_df.to_dict('records')
            except:
                pass

        if str(response.url)=='https://www.ok.gov/able/documents/JULY%2C%202019%20%28DIS%29.pdf':
            try:
                df = getDf(pdf_link,[91.575,25.74,571.725,748.44],[63.36,95,162.36,261.36,366.3,480,560.16,580,610,670,748.44])
                df.columns = ['license_no', 'type', 'county', 'dba_name', 'license_name', 'address', 'city', 'state','zip_code', 'company_phone', 'permit_lic_exp_date']
                df['license_no'] = df['license_no'].fillna(method='bfill')
                df['boolean'] = df.license_no.eq(df.license_no.shift())
                df['license_no'] = df.apply(lambda x:x['boolean'] if not x[11]==False else x[0], axis = 1)
                df = df.drop(['boolean'], axis=1)
                groups = df.groupby(df['license_no'].apply(rolling_group), as_index=False)
                groupFunct = lambda g: pd.Series([joinFunc(g, col) for col in g.columns], index=g.columns)
                df['license_no'] = df['license_no'].replace(True, '')
                final_df = groups.apply(groupFunct).fillna('')
                return final_df.to_dict('records')
            except:
                pass

        if str(response.url)=='https://www.ok.gov/able/documents/JULY%2C%202019%20%28MXB%29.pdf':
            try:
                df = getDf(pdf_link,[86.625,26.73,574.695,760.32],[61,94.05,143.55,302.94,426.69,535.59,600.93,620,649.44,705,760])
                df.columns = ['license_no', 'type', 'county', 'dba_name', 'license_name', 'address', 'city', 'state','zip_code', 'company_phone', 'permit_lic_exp_date']
                df['license_no'] = df['license_no'].fillna(method='bfill')
                df['boolean'] = df.license_no.eq(df.license_no.shift())
                df['license_no'] = df.apply(lambda x:x['boolean'] if not x[11]==False else x[0], axis = 1)
                df = df.drop(['boolean'], axis=1)
                groups = df.groupby(df['license_no'].apply(rolling_group), as_index=False)
                groupFunct = lambda g: pd.Series([joinFunc(g, col) for col in g.columns], index=g.columns)
                df['license_no'] = df['license_no'].replace(True, '')
                final_df = groups.apply(groupFunct).fillna('')
                return final_df.to_dict('records')
            except:
                pass

        if str(response.url)=='https://www.ok.gov/able/documents/JULY%2C%202019%20%28MXF%29.pdf':
            try:
                df = getDf(pdf_link,[86.625,26.73,574.695,760.32],[61,94.05,143.55,268,360,510,600.93,620,649.44,715,760])
                df.columns = ['license_no', 'type', 'county', 'dba_name', 'license_name', 'address', 'city', 'state','zip_code', 'company_phone', 'permit_lic_exp_date']
                df['license_no'] = df['license_no'].fillna(method='bfill')
                df['boolean'] = df.license_no.eq(df.license_no.shift())
                df['license_no'] = df.apply(lambda x:x['boolean'] if not x[11]==False else x[0], axis = 1)
                df = df.drop(['boolean'], axis=1)
                groups = df.groupby(df['license_no'].apply(rolling_group), as_index=False)
                groupFunct = lambda g: pd.Series([joinFunc(g, col) for col in g.columns], index=g.columns)
                df['license_no'] = df['license_no'].replace(True, '')
                final_df = groups.apply(groupFunct).fillna('')
                return final_df.to_dict('records')
            except:
                pass

        if str(response.url)=='https://www.ok.gov/able/documents/JULY%2C%202019%20%28NRS%29.pdf':
            try:
                df = getDf(pdf_link,[86.625,26.73,574.695,760.32],[61,94.05,143.55,268,400,515,600,620,650,710,760])
                df.columns = ['license_no', 'type', 'county', 'dba_name', 'license_name', 'address', 'city', 'state','zip_code', 'company_phone', 'permit_lic_exp_date']
                df['license_no'] = df['license_no'].fillna(method='bfill')
                df['boolean'] = df.license_no.eq(df.license_no.shift())
                df['license_no'] = df.apply(lambda x:x['boolean'] if not x[11]==False else x[0], axis = 1)
                df = df.drop(['boolean'], axis=1)
                groups = df.groupby(df['license_no'].apply(rolling_group), as_index=False)
                groupFunct = lambda g: pd.Series([joinFunc(g, col) for col in g.columns], index=g.columns)
                df['license_no'] = df['license_no'].replace(True, '')
                final_df = groups.apply(groupFunct).fillna('')
                return final_df.to_dict('records')
            except:
                pass

        if str(response.url)=='https://www.ok.gov/able/documents/JULY%2C%202019%20%28OKW%29.pdf':
            try:
                df = getDf(pdf_link,[94.545,36.135,407.385,736.065],[75,99.9,158.4,278.19,410,490,575,590,620,680,730])
                df.columns = ['license_no', 'type', 'county', 'dba_name', 'license_name', 'address', 'city', 'state','zip_code', 'company_phone', 'permit_lic_exp_date']
                df['license_no'] = df['license_no'].fillna(method='bfill')
                df['boolean'] = df.license_no.eq(df.license_no.shift())
                df['license_no'] = df.apply(lambda x:x['boolean'] if not x[11]==False else x[0], axis = 1)
                df = df.drop(['boolean'], axis=1)
                groups = df.groupby(df['license_no'].apply(rolling_group), as_index=False)
                groupFunct = lambda g: pd.Series([joinFunc(g, col) for col in g.columns], index=g.columns)
                df['license_no'] = df['license_no'].replace(True, '')
                final_df = groups.apply(groupFunct).fillna('')
                return final_df.to_dict('records')
            except:
                pass

        if str(response.url)=='https://www.ok.gov/able/documents/JULY%2C%202019%20%28REB%29.pdf':
            try:
                df = getDf(pdf_link,[91.575,27.72,568.755,765.27],[61,94.05,150,268,400,520,600,620,650,710,760])
                df.columns = ['license_no', 'type', 'county', 'dba_name', 'license_name', 'address', 'city', 'state','zip_code', 'company_phone', 'permit_lic_exp_date']
                df['license_no'] = df['license_no'].fillna(method='bfill')
                df['boolean'] = df.license_no.eq(df.license_no.shift())
                df['license_no'] = df.apply(lambda x:x['boolean'] if not x[11]==False else x[0], axis = 1)
                df = df.drop(['boolean'], axis=1)
                groups = df.groupby(df['license_no'].apply(rolling_group), as_index=False)
                groupFunct = lambda g: pd.Series([joinFunc(g, col) for col in g.columns], index=g.columns)
                df['license_no'] = df['license_no'].replace(True, '')
                final_df = groups.apply(groupFunct).fillna('')
                return final_df.to_dict('records')
            except:
                pass

        if str(response.url)=='https://www.ok.gov/able/documents/JULY%2C%202019%20%28REW%29.pdf':
            try:
                df = getDf(pdf_link,[91.575,27.72,568.755,765.27],[61,94.05,175,268,420,520,600,620,650,710,760])
                df.columns = ['license_no', 'type', 'county', 'dba_name', 'license_name', 'address', 'city', 'state','zip_code', 'company_phone', 'permit_lic_exp_date']
                df['license_no'] = df['license_no'].fillna(method='bfill')
                df['boolean'] = df.license_no.eq(df.license_no.shift())
                df['license_no'] = df.apply(lambda x:x['boolean'] if not x[11]==False else x[0], axis = 1)
                df = df.drop(['boolean'], axis=1)
                groups = df.groupby(df['license_no'].apply(rolling_group), as_index=False)
                groupFunct = lambda g: pd.Series([joinFunc(g, col) for col in g.columns], index=g.columns)
                df['license_no'] = df['license_no'].replace(True, '')
                final_df = groups.apply(groupFunct).fillna('')
                return final_df.to_dict('records')
            except:
                pass
        if str(response.url)=='https://www.ok.gov/able/documents/JULY%2C%202019%20%28RET%29.pdf':
            try:
                df = getDf(pdf_link,[91.575,27.72,568.755,765.27],[61,94,150,280,420,535,600,620,650,710,760])
                df.columns = ['license_no', 'type', 'county', 'dba_name', 'license_name', 'address', 'city', 'state','zip_code', 'company_phone', 'permit_lic_exp_date']
                df['license_no'] = df['license_no'].fillna(method='bfill')
                df['boolean'] = df.license_no.eq(df.license_no.shift())
                df['license_no'] = df.apply(lambda x:x['boolean'] if not x[11]==False else x[0], axis = 1)
                df = df.drop(['boolean'], axis=1)
                groups = df.groupby(df['license_no'].apply(rolling_group), as_index=False)
                groupFunct = lambda g: pd.Series([joinFunc(g, col) for col in g.columns], index=g.columns)
                df['license_no'] = df['license_no'].replace(True, '')
                final_df = groups.apply(groupFunct).fillna('')
                return final_df.to_dict('records')
            except:
                pass

        if str(response.url)=='https://www.ok.gov/able/documents/JULY%2C%202019%20%28SED%29.pdf':
            try:
                df = getDf(pdf_link,[91.575,27.72,568.755,765.27],[61,94,150,280,370,500,560,610,635,700,760])
                df.columns = ['license_no', 'type', 'county', 'dba_name', 'license_name', 'address', 'city', 'state','zip_code', 'company_phone', 'permit_lic_exp_date']
                df['license_no'] = df['license_no'].fillna(method='bfill')
                df['boolean'] = df.license_no.eq(df.license_no.shift())
                df['license_no'] = df.apply(lambda x:x['boolean'] if not x[11]==False else x[0], axis = 1)
                df = df.drop(['boolean'], axis=1)
                groups = df.groupby(df['license_no'].apply(rolling_group), as_index=False)
                groupFunct = lambda g: pd.Series([joinFunc(g, col) for col in g.columns], index=g.columns)
                df['license_no'] = df['license_no'].replace(True, '')
                final_df = groups.apply(groupFunct).fillna('')
                return final_df.to_dict('records')
            except:
                pass

        if str(response.url)=='https://www.ok.gov/able/documents/JULY%2C%202019%20%28OKB%29.pdf':
            try:
                df = getDf(pdf_link,[91.575,27.72,568.755,765.27],[65,94,150,280,410,520,560,620,640,700,760])
                df.columns = ['license_no', 'type', 'county', 'dba_name', 'license_name', 'address', 'city', 'state','zip_code', 'company_phone', 'permit_lic_exp_date']
                df['license_no'] = df['license_no'].fillna(method='bfill')
                df['boolean'] = df.license_no.eq(df.license_no.shift())
                df['license_no'] = df.apply(lambda x:x['boolean'] if not x[11]==False else x[0], axis = 1)
                df = df.drop(['boolean'], axis=1)
                groups = df.groupby(df['license_no'].apply(rolling_group), as_index=False)
                groupFunct = lambda g: pd.Series([joinFunc(g, col) for col in g.columns], index=g.columns)
                df['license_no'] = df['license_no'].replace(True, '')
                final_df = groups.apply(groupFunct).fillna('')
                return final_df.to_dict('records')
            except:
                pass

        if str(response.url)=='https://www.ok.gov/able/documents/JULY%2C%202019%20%28SFW%29.pdf':
            try:
                df = getDf(pdf_link,[93.555,26.235,256.905,704.385],[67.815,109.395,168.795,256.905,350,460,530,560,591.525,653.895,704])
                df.columns = ['license_no', 'type', 'county', 'dba_name', 'license_name', 'address', 'city', 'state','zip_code', 'company_phone', 'permit_lic_exp_date']
                df['license_no'] = df['license_no'].fillna(method='bfill')
                df['boolean'] = df.license_no.eq(df.license_no.shift())
                df['license_no'] = df.apply(lambda x:x['boolean'] if not x[11]==False else x[0], axis = 1)
                df = df.drop(['boolean'], axis=1)
                groups = df.groupby(df['license_no'].apply(rolling_group), as_index=False)
                groupFunct = lambda g: pd.Series([joinFunc(g, col) for col in g.columns], index=g.columns)
                df['license_no'] = df['license_no'].replace(True, '')
                final_df = groups.apply(groupFunct).fillna('')
                return final_df.to_dict('records')
            except:
                pass

        if str(response.url)=='https://www.ok.gov/able/documents/JULY%2C%202019%20%28WSW%29.pdf':
            try:
                df = getDf(pdf_link,[93.555,28.215,339.075,725.175],[67,103,164,265,364,478,560,581,605,669,722])
                df.columns = ['license_no', 'type', 'county', 'dba_name', 'license_name', 'address', 'city', 'state','zip_code', 'company_phone', 'permit_lic_exp_date']
                df['license_no'] = df['license_no'].fillna(method='bfill')
                df['boolean'] = df.license_no.eq(df.license_no.shift())
                df['license_no'] = df.apply(lambda x:x['boolean'] if not x[11]==False else x[0], axis = 1)
                df = df.drop(['boolean'], axis=1)
                groups = df.groupby(df['license_no'].apply(rolling_group), as_index=False)
                groupFunct = lambda g: pd.Series([joinFunc(g, col) for col in g.columns], index=g.columns)
                df['license_no'] = df['license_no'].replace(True, '')
                final_df = groups.apply(groupFunct).fillna('')
                return final_df.to_dict('records')
            except:
                pass

        if str(response.url)=='https://www.ok.gov/able/documents/JULY%2C%202019%20%28CMB%29.pdf':
            try:
                df = getDf(pdf_link,[85.635,28.71,580.635,758.34],[66.33,98.0,162.36,295,413.82,526.68,593,621,649,710,757])
                df.columns = ['license_no', 'type', 'county', 'dba_name', 'license_name', 'address', 'city', 'state','zip_code', 'company_phone', 'permit_lic_exp_date']
                df['license_no'] = df['license_no'].fillna(method='bfill')
                df['boolean'] = df.license_no.eq(df.license_no.shift())
                df['license_no'] = df.apply(lambda x:x['boolean'] if not x[11]==False else x[0], axis = 1)
                df = df.drop(['boolean'], axis=1)
                groups = df.groupby(df['license_no'].apply(rolling_group), as_index=False)
                groupFunct = lambda g: pd.Series([joinFunc(g, col) for col in g.columns], index=g.columns)
                df['license_no'] = df['license_no'].replace(True, '')
                final_df = groups.apply(groupFunct).fillna('')
                return final_df.to_dict('records')
            except:
                pass

        if str(response.url)=='https://www.ok.gov/able/documents/JULY%2C%202019%20%28WIN%29.pdf':
            try:
                df = getDf(pdf_link,[95.535,28.71,568.755,737.55],[70,100,162.36,260,370,490,550,590,620,685,735])
                df.columns = ['license_no', 'type', 'county', 'dba_name', 'license_name', 'address', 'city', 'state','zip_code', 'company_phone', 'permit_lic_exp_date']
                df['license_no'] = df['license_no'].fillna(method='bfill')
                df['boolean'] = df.license_no.eq(df.license_no.shift())
                df['license_no'] = df.apply(lambda x:x['boolean'] if not x[11]==False else x[0], axis = 1)
                df = df.drop(['boolean'], axis=1)
                groups = df.groupby(df['license_no'].apply(rolling_group), as_index=False)
                groupFunct = lambda g: pd.Series([joinFunc(g, col) for col in g.columns], index=g.columns)
                df['license_no'] = df['license_no'].replace(True, '')
                final_df = groups.apply(groupFunct).fillna('')
                return final_df.to_dict('records')
            except:
                pass


    def parse_pdf(self,response):
        meta={}
        meta['permit_subtype']=meta['permit_lic_no']= meta['dba_name']=meta['county']=meta['company_name']=meta['company_phone']=meta['add']=meta['city']=meta['state']=meta['permit_lic_exp_date']=meta['location_address_string']=meta['zip_code']=''
        for col in self.__extractData(response):
            # print("--------------------------------------",col)
            meta['permit_subtype']=response.meta['permit_subtype']
            meta['permit_lic_no']=col['license_no']
            meta['dba_name']=col['dba_name']
            meta['county']=col['county']
            meta['company_name']=col['license_name']
            meta['add']=col['address']
            meta['city']=col['city']
            meta['state']=col['state']
            meta['zip_code']=col['zip_code']
            meta['company_phone']=col['company_phone']
            meta['permit_lic_exp_date']=col['permit_lic_exp_date']
            meta['location_address_string']=self.format__address_4(meta['add'],meta['city'],meta['state'],meta['zip_code'])
            yield self.save_to_csv(response,**meta)
            
    
    def save_to_csv(self,response,**meta):
        print('------------------------------------------------->',response.url)
        # self.state['items_count'] = self.state.get('items_count', 0) + 1
        # il.default_input_processor = MapCompose(lambda v: v.strip(), remove_tags, replace_escape_chars)
        il = ItemLoader(item=OkAlcoholLicensesSpiderItem())
        if str(response.url)=='https://www.ok.gov/able/documents/JULY%2C%202019%20%28BAW%29.pdf':
            meta['permit_subtype']=' BEER AND WINE'
        elif str(response.url)=='https://www.ok.gov/able/documents/JULY%2C%202019%20%28BED%29.pdf':
            meta['permit_subtype']=' BEER DISTRIBUTORS'
        elif str(response.url)=='https://www.ok.gov/able/documents/JULY%2C%202019%20%28BWH%29.pdf':
            meta['permit_subtype']='BONDED WAREHOUSE'
        elif str(response.url)=='https://www.ok.gov/able/documents/JULY%2C%202019%20%28BRP%29.pdf':
            meta['permit_subtype']='BREWPUB'
        elif str(response.url)=='https://www.ok.gov/able/documents/JULY%2C%202019%20%28CAT%29.pdf':
            meta['permit_subtype']=' CATERERS'
        elif str(response.url)=='https://www.ok.gov/able/documents/JULY%2C%202019%20%28DSP%29.pdf':
            meta['permit_subtype']='Direct Wine Shipper Permit '
        elif str(response.url)=='https://www.ok.gov/able/documents/JULY%2C%202019%20%28DIS%29.pdf':
            meta['permit_subtype']='Distiller '
        elif str(response.url)=='https://www.ok.gov/able/documents/JULY%2C%202019%20%28MXB%29.pdf':
            meta['permit_subtype']='Mixed Beverage'
        elif str(response.url)=='https://www.ok.gov/able/documents/JULY%2C%202019%20%28CMB%29.pdf':
            meta['permit_subtype']='Mixed Beverage /Cater'
        elif str(response.url)=='https://www.ok.gov/able/documents/JULY%2C%202019%20%28MXF%29.pdf':
            meta['permit_subtype']='Mixed Beverage Fraternal'
        elif str(response.url)=='https://www.ok.gov/able/documents/JULY%2C%202019%20%28NRS%29.pdf':
            meta['permit_subtype']='Nonresident Seller/Manufacturer'
        elif str(response.url)=='https://www.ok.gov/able/documents/JULY%2C%202019%20%28OKW%29.pdf':
            meta['permit_subtype']='Oklahoma Winemakers'
        elif str(response.url)=='https://www.ok.gov/able/documents/JULY%2C%202019%20%28REB%29.pdf':
            meta['permit_subtype']='Retail Beer'
        elif str(response.url)=='https://www.ok.gov/able/documents/JULY%2C%202019%20%28REW%29.pdf':
            meta['permit_subtype']='Retail Wine'
        elif str(response.url)=='https://www.ok.gov/able/documents/JULY%2C%202019%20%28RET%29.pdf':
            meta['permit_subtype']='Retail Spirits '
        elif str(response.url)=='https://www.ok.gov/able/documents/JULY%2C%202019%20%28SED%29.pdf':
            meta['permit_subtype']='Self Distribution'
        elif str(response.url)=='https://www.ok.gov/able/documents/JULY%2C%202019%20%28OKB%29.pdf':
            meta['permit_subtype']='Oklahoma Brewer'
        elif str(response.url)=='https://www.ok.gov/able/documents/JULY%2C%202019%20%28SFW%29.pdf':
            meta['permit_subtype']='Small Farm Winery '
        elif str(response.url)=='https://www.ok.gov/able/documents/JULY%2C%202019%20%28WSW%29.pdf':
            meta['permit_subtype']='Wine and Spirits Wholesaler '
        elif str(response.url)=='https://www.ok.gov/able/documents/JULY%2C%202019%20%28WIN%29.pdf':
            meta['permit_subtype']='Winemaker'
        il.add_value('ingestion_timestamp', Utils.getingestion_timestamp())
        il.add_value('url', 'https://www.ok.gov/able/Monthly__Reports/index.html')
        il.add_value('sourceName', 'OK_Alcohol_Licenses')
        il.add_value('dba_name',meta['dba_name'])
        il.add_value('county',meta['county'])
        il.add_value('company_phone',meta['company_phone'])
        il.add_value('location_address_string',meta['location_address_string'])
        il.add_value('permit_lic_no',meta['permit_lic_no'])
        il.add_value('permit_lic_exp_date',meta['permit_lic_exp_date'])
        il.add_value('permit_subtype',meta['permit_subtype'])
        il.add_value('permit_type', 'Liquor_Licenses')
        il.add_value('company_name',meta['company_name'])
        il.add_value('city',meta['city'])
        il.add_value('state',meta['state'])
        il.add_value('zip_code',meta['zip_code'])
        return il.load_item()