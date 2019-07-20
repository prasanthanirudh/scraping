# -*- coding: utf-8 -*-

'''
Created on 2018-Dec-05 06:07:18
TICKET NUMBER -AI_710
@author: ait-python
'''
import datetime
import pdfquery
import tabula
import pandas as pd
from OpenSSL import SSL
import scrapy
import re
import glob
from scrapy.loader import ItemLoader
from scrapy.loader.processors import MapCompose
from w3lib.html import remove_tags, replace_escape_chars

from Data_scuff.spiders.AI_710.items import CtForestPractitionerLicenseSpiderItem
from Data_scuff.spiders.__common import CommonSpider,CustomSettings
from Data_scuff.utils.utils import Utils

class CtForestPractitionerLicenseSpider(CommonSpider):
    name = 'ct_forest_practitioner_license'
    allowed_domains = ['ct.gov']
    start_urls = ['https://www.depdata.ct.gov/forestry/ForestPractitioner/directry.pdf']
    
    custom_settings = {
        'FILE_NAME':Utils.getRundateFileName('Licenses_ForestPractitioner_CT_CurationReady'),
        'JIRA_ID':'AI_710',
        # 'JOBDIR' : CustomSettings.getJobDirectory('CtForestPractitionerLicenseSpider'),
        'TOP_HEADER':{                        'extended permit': 'Extended Permit',
                         'level': 'Level',
                         'location_address_string': 'Address',
                         'permit_lic_desc': '',
                         'permit_lic_exp_date': 'Expiration',
                         'permit_lic_no': 'Cert. #',
                         'permit_subtype': '',
                         'permit_type': '',
                         'person_name': 'Name',
                         'person_phone': 'Phone'},
        'FIELDS_TO_EXPORT':['person_name', 
                            'location_address_string',
                            'person_phone', 
                            'level',
                            'permit_subtype',
                            'permit_lic_no',
                            'permit_lic_exp_date',
                            'extended permit',
                            'permit_lic_desc', 
                            'permit_type', 
                            'sourceName',
                            'url',
                            'ingestion_timestamp'],
        'NULL_HEADERS':['level', 'extended permit']
        }

    def parse(self, response):
        yield scrapy.Request(url= 'https://www.depdata.ct.gov/forestry/ForestPractitioner/directry.pdf', callback = self.parse_pdf, dont_filter=True)

    def __extractData(self,response):
        def rolling_group(val):
            if pd.notnull(val): 
            # if pd.notnull(val) and '/' in val and not 'st' in val:
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

        def getDf(temp_file, area):
            return tabula.read_pdf(temp_file,
                pages='8-27',
                Stream=True,
                silent=True,
                guess=False,
                columns=[95.625,173.655,241.74,315.18,341.955,372.555,437.58,467.415,508.725,597.465],
                encoding='ISO-8859-1',
                area=area,
                pandas_options={'header': 'infer'}
                ).replace('\r', ' ', regex=True).dropna(how='all')
        df = getDf('https://www.depdata.ct.gov/forestry/ForestPractitioner/directry.pdf',[65.611,19.89,731.161,598.23])

        df.columns = ['l_name','f_name','address','city','state','zip','phone','level','cert','expiration'] 
        groups = df.groupby(df['expiration'].apply(rolling_group), as_index=False)
        groupFunct = lambda g: pd.Series([joinFunc(g, col) for col in g.columns], index=g.columns)
        final_df = groups.apply(groupFunct).fillna('')
        yield final_df.to_dict('records')
    


    def parse_pdf(self, response):
        for row in self.__extractData(response):
            for col in row:
                # d = re.search(r"[\d]/[\d]/[\d]$", col['expiration'])
                # if d:
                # self.state['items_count'] = self.state.get('items_count', 0) + 1
                il = ItemLoader(item=CtForestPractitionerLicenseSpiderItem())
                il.default_input_processor = MapCompose(lambda v: v.strip(), remove_tags, replace_escape_chars)
                il.add_value('ingestion_timestamp', Utils.getingestion_timestamp())
                il.add_value('url', 'https://www.depdata.ct.gov/forestry/ForestPractitioner/directry.pdf')
                il.add_value('sourceName', 'CT_Forest_Practitioner_License')
                il.add_value('person_phone', col['phone'])
                name = col['f_name']+' '+col['l_name']
                il.add_value('person_name', name)
                if ' ' in col['expiration']:
                    date = col['expiration'].split(' ')[0]
                    e_permit =col['expiration'].split(' ')[1]
                else:
                    date =col['expiration']
                    e_permit =''

                print("@@@@@@@@@@@@@@@@@@@@@@@@@@@@@2",date)
                il.add_value('permit_lic_exp_date', date)
                if '490' in e_permit:
                    e_permit="490- permitted to assist landowners seeking classification of their land as 'Forest Land'"
                il.add_value('extended permit', e_permit)
                il.add_value('permit_lic_no', col['cert'])
                level_desc = col['level']
                if level_desc =='F':
                    level_desc='FORESTER'
                elif level_desc =='SFPH':
                    level_desc='SUPERVISING FOREST PRODUCTS HARVESTER'
                elif level_desc == 'FPH':
                    level_desc = 'FOREST PRODUCTS HARVESTER'
                il.add_value('level', col['level'])
                il.add_value('permit_subtype', level_desc)
                il.add_value('permit_lic_desc', level_desc)
                il.add_value('permit_type', 'forester_license')
                location_address_string = col['address']+', '+ col['city']+', '+col['state']+' '+ col['zip']
                il.add_value('location_address_string', location_address_string)
                yield il.load_item()