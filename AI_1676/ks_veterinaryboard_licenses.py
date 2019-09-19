# -*- coding: utf-8 -*-

'''
Created on 2019-Aug-22 05:31:01
TICKET NUMBER -AI_1676
@author: ait-python
'''

from scrapy.loader import ItemLoader
from scrapy.loader.processors import MapCompose
from w3lib.html import remove_tags, replace_escape_chars

from Data_scuff.spiders.AI_1676.items import KsVeterinaryboardLicensesSpiderItem
from Data_scuff.spiders.__common import CommonSpider,CustomSettings
from Data_scuff.utils.utils import Utils

import scrapy
import tabula
import pandas as pd
import re
import numpy as np
from PyPDF2 import PdfFileReader

class KsVeterinaryboardLicensesSpider(CommonSpider):
    name = '1676_ks_veterinaryboard_licenses'
    allowed_domains = ['ks.gov']
    start_urls = ['http://agriculture.ks.gov/divisions-programs/division-of-animal-health/kansas-board-of-veterinary-examiners/licensee-information']
    
    custom_settings = {
        'FILE_NAME':Utils.getRundateFileName('AI_1676_Licenses_VeterinaryBoard_KS_CurationReady'),
        'JIRA_ID':'AI_1676',
        'COOKIES_ENABLED':True,
        'COOKIES_DEBUG':True,
        'HTTPCACHE_ENABLED':False,
        # 'JOBDIR' : CustomSettings.getJobDirectory('ks_veterinaryboard_licenses'),
        'TOP_HEADER':{'company_name': 'VETERINARIAN/Contact', 'dba_name': '', 'location_address_string': '', 'permit_applied_date': 'YR Registd', 'permit_lic_desc': '', 'permit_lic_eff_date': 'DATE ISSUED', 'permit_type': ''},
        'FIELDS_TO_EXPORT':[
            'company_name', 
            'dba_name', 
            'location_address_string', 
            'permit_lic_eff_date', 
            'permit_applied_date', 
            'permit_lic_desc', 
            'permit_type', 
            'url',
            'sourceName', 
            'ingestion_timestamp', 
            ],
        'NULL_HEADERS':[]
        }

    def parse(self, response):
        meta={}
        file1='https://kbve.kansas.gov/wp-content/uploads/2017/09/Current_RVTs.pdf'
        file2='https://kbve.kansas.gov/wp-content/uploads/2017/09/currently_not_licensed.pdf'
        file3='https://kbve.kansas.gov/wp-content/uploads/2017/09/newly_licensed.pdf'
        file4='https://kbve.kansas.gov/wp-content/uploads/2017/09/currently_licensed.pdf'
        meta['company_name']=meta['location_address_string']=meta['permit_applied_date']=meta['permit_lic_desc']=meta['permit_lic_eff_date']=''

        df = tabula.read_pdf(file1,
        pages ='all',
        area = [92.948,49.725,731.723,530.145],
        # silent=True,
        guess = False,
        encoding='utf-8',
        pandas_option = {'header':None})
        for _, row in df.fillna('').iterrows():
            row=row.tolist()
            meta['company_name']=row[2]
            meta['location_address_string']='KS'
            meta['permit_applied_date']=row[3]
            meta['permit_lic_desc']='Veterinary License for '+meta['company_name']
            # print(meta['company_name'])
            yield self.save_to_csv(response,**meta).load_item()


        df = tabula.read_pdf(file2,
        pages ='all',
        area = [79.178,47.43,731.723,474.3],
        silent=True,
        guess = False,
        encoding='utf-8',
        pandas_option = {'header':None})
        for _, row in df.fillna('').iterrows():
            row=row.tolist()
            meta['company_name']=row[1]
            meta['permit_lic_eff_date']=row[2]
            meta['location_address_string']='KS'
            meta['permit_applied_date']=''
            meta['permit_lic_desc']='Veterinary License for '+meta['company_name']
            yield self.save_to_csv(response,**meta).load_item()


        df = tabula.read_pdf(file3,
        pages ='all',
        area = [132.728,52.02,727.898,445.23],
        silent=True,
        guess = False,
        encoding='utf-8',
        pandas_option = {'header':None})
        for _, row in df.fillna('').iterrows():
            row=row.tolist()
            meta['company_name']=row[1]
            meta['permit_lic_eff_date']=row[2]
            meta['location_address_string']='KS'
            meta['permit_applied_date']=''
            meta['permit_lic_desc']='Veterinary License for '+meta['company_name']
            yield self.save_to_csv(response,**meta).load_item()



        df = tabula.read_pdf(file4,
        pages ='all',
        area = [92.948,19.125,750.083,388.62],
        silent=True,
        guess = False,
        encoding='utf-8',
        pandas_option = {'header':None})
        for _, row in df.fillna('').iterrows():
            row=row.tolist()
            meta['company_name']=row[1]
            meta['permit_lic_eff_date']=row[2]
            meta['location_address_string']='KS'
            meta['permit_applied_date']=''
            meta['permit_lic_desc']='Veterinary License for '+meta['company_name']
            yield self.save_to_csv(response,**meta).load_item()





    def save_to_csv(self,response, **meta):
        # self.state['items_count'] = self.state.get('items_count', 0) + 1
        il = ItemLoader(item=KsVeterinaryboardLicensesSpiderItem(),response=response)
        # il.default_input_processor = MapCompose(lambda v: v.strip(), remove_tags, replace_escape_chars)
        #il.add_value('ingestion_timestamp', Utils.getingestion_timestamp())
        il.add_value('sourceName', 'KS_VeterinaryBoard_Licenses')
        il.add_value('url', 'http://agriculture.ks.gov/divisions-programs/division-of-animal-health/kansas-board-of-veterinary-examiners/licensee-information')
        il.add_value('company_name',self._getDBA(meta['company_name'])[0])
        il.add_value('permit_lic_desc',meta['permit_lic_desc'])
        il.add_value('permit_applied_date',meta['permit_applied_date'])
        il.add_value('location_address_string', meta['location_address_string'])
        il.add_value('permit_lic_eff_date', meta['permit_lic_eff_date'])
        il.add_value('permit_type', 'veterinary_license')
        il.add_value('dba_name', self._getDBA(meta['company_name'])[1])
        return il