# -*- coding: utf-8 -*-

'''
Created on 2019-Jul-11 06:04:44
TICKET NUMBER -AI_1151
@author: ait-python
'''

from scrapy.loader import ItemLoader
from scrapy.loader.processors import MapCompose
from w3lib.html import remove_tags, replace_escape_chars

from Data_scuff.spiders.AI_1151.items import OrPharmacyFacilityLicensesSpiderItem
from Data_scuff.spiders.__common import CommonSpider,CustomSettings
from Data_scuff.utils.utils import Utils
import tempfile
import scrapy
import os
import tabula
import pandas as pd
import re
import numpy as np
from PyPDF2 import PdfFileReader
pd.set_option('display.max_rows', 200)
pd.set_option('display.max_columns', 200)
pd.set_option('display.width', 200)


class OrPharmacyFacilityLicensesSpider(CommonSpider):
    name = 'or_pharmacy_facility_licenses'
    allowed_domains = ['oregon.gov']
    start_urls = ['http://www.oregon.gov/Pharmacy/Imports/List_Pharmacies_by_State_County.pdf']
    
    custom_settings = {
        'FILE_NAME':Utils.getRundateFileName('AI-1151_Licenses_Pharmacy_Facility_OR_CurationReady'),
        'JIRA_ID':'AI_1151',
        'HTTPCACHE_ENABLED': False,
        'COOKIES_ENABLED':True,
        # 'JOBDIR' : CustomSettings.getJobDirectory('or_pharmacy_facility_licenses'),
        'TOP_HEADER':{                        
                         'permit_lic_no': 'License Number',
                         'company_name': 'Facility Name',
                         'dba_name': '',
                         'location_Address_string': 'Location Address',
                         'permit_lic_status': 'License Status',
                         'permit_lic_exp_date': 'Expiration Date',
                         'permit_lic_desc': '',
                         'permit_type': ''},

        'FIELDS_TO_EXPORT':[
                            'permit_lic_no', 
                            'company_name', 
                            'dba_name', 
                            'location_Address_string', 
                            'permit_lic_status', 
                            'permit_lic_exp_date', 
                            'permit_lic_desc', 
                            'permit_type', 
                            'sourceName',
                            'url',
                            'ingestion_timestamp'], 
        'NULL_HEADERS':[]
        }

    def parse(self, response):

        df = tabula.read_pdf('https://www.oregon.gov/Pharmacy/Imports/List_Pharmacies_by_State_County.pdf',
            pages ='all',
            area = [9.405,8.91,596.475,773.19],
            columns=[77.22,272.25,466.29,555.39,598.95,637.56,680.03,766.26],
            silent=True,
            guess = False,
            encoding='ISO-8859-1',
            pandas_option = {'header':None})
        
        for _, row in df.fillna('').iterrows():
            permit_lic_no=row[0]  
            permit_lic_no=re.search(r'[A-Z]{2}-[0-9]*',permit_lic_no)
            if permit_lic_no:
                permit_lic_no=permit_lic_no.group()
            company_name=row[1]
            location=row[2]
            state=row[3]
            city=row[4]
            code=row[5]
            location_address_string=str(location)+', '+str(state)+', '+str(city)+' '+str(code)
            permit_lic_status=row[6]
            permit_lic_exp_date=row[7]
            permit_lic_desc='Pharmacy License for '+company_name


            # self.state['items_count'] = self.state.get('items_count', 0) + 1
            il = ItemLoader(item=OrPharmacyFacilityLicensesSpiderItem())
            # il.default_input_processor = MapCompose(lambda v: v.strip(), remove_tags, replace_escape_chars)
            #il.add_value('ingestion_timestamp', Utils.getingestion_timestamp())
            il.add_value('url', 'http://www.oregon.gov/Pharmacy/Imports/List_Pharmacies_by_State_County.pdf')
            il.add_value('sourceName', 'OR_Pharmacy_Facility_Licenses')
            il.add_value('permit_lic_exp_date', permit_lic_exp_date)
            il.add_value('company_name',self._getDBA(company_name)[0])
            il.add_value('location_Address_string',location_address_string)
            il.add_value('dba_name',self._getDBA(company_name)[1])
            il.add_value('permit_lic_status', permit_lic_status)
            il.add_value('permit_lic_no', permit_lic_no)
            il.add_value('permit_type', 'pharmacy_license')
            il.add_value('permit_lic_desc',permit_lic_desc)
            yield il.load_item()

    def storeGet_tempfile(self,response):
        outfd, temp_path = tempfile.mkstemp(prefix='', suffix='')
        with os.fdopen(outfd, 'wb') as pdf_file:
            pdf_file.write(response.body)
        return temp_path