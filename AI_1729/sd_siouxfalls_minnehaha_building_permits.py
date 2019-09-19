# -*- coding: utf-8 -*-

'''
Created on 2019-Aug-29 07:21:25
TICKET NUMBER -AI_1729
@author: ait-python
'''

from scrapy.loader import ItemLoader
from scrapy.loader.processors import MapCompose
from w3lib.html import remove_tags, replace_escape_chars

from Data_scuff.spiders.AI_1729.items import SdSiouxfallsMinnehahaBuildingPermitsSpiderItem
from Data_scuff.spiders.__common import CommonSpider,CustomSettings
from Data_scuff.utils.utils import Utils
from Data_scuff.utils.JavaScriptUtils import JavaScriptUtils
import scrapy 

class SdSiouxfallsMinnehahaBuildingPermitsSpider(CommonSpider):
    name = '1729_sd_siouxfalls_minnehaha_building_permits'
    allowed_domains = ['siouxfalls.org']
    start_urls = ['https://webapps.siouxfalls.org/permits/']
    
    custom_settings = {
        'FILE_NAME':Utils.getRundateFileName('AI-1729_Permits_Buildings_SD_SiouxFalls_Minnehaha_CurationReady_'),
        'JIRA_ID':'AI_1729',
        'HTTPCACHE_ENABLED':False,
        'COOKIES_ENABLED':True,
        'COOKIES_DEBUG':True,
        # 'JOBDIR' : CustomSettings.getJobDirectory('sd_siouxfalls_minnehaha_building_permits'),
        'TOP_HEADER':{   'contractor_lic_no': '',
                         'location_address_string': 'House # + Street',
                         'mixed_contractor_name': 'Contractor',
                         'permit_lic_desc': 'Description',
                         'permit_lic_eff_date': 'Date',
                         'permit_lic_no': 'Permit #',
                         'permit_lic_value': 'Value',
                         'permit_subtype': 'Type',
                         'permit_type': ''},
        'FIELDS_TO_EXPORT':[
                            'permit_lic_eff_date', 
                            'permit_lic_no',
                            'location_address_string', 
                            'permit_subtype',
                            'permit_lic_desc', 
                            'mixed_contractor_name', 
                            'contractor_dba',
                            'contractor_lic_no',
                            'permit_lic_value', 
                            'permit_type', 
                            'sourceName', 
                            'url', 
                            'ingestion_timestamp', 
                            ],

        'NULL_HEADERS':[]
        }
    page=1
    def parse(self, response):
        # print("------------------------",response.text)
        contractor_lic_no=mixed_contractor_name=''
        main_data=response.xpath('//*[@class="datagrid"]//tr[@class="DataRow" or @class="altDataRow"]')
        for i in main_data:
            permit_lic_eff_date=i.xpath('td[1]/text()').extract_first()
            permit_lic_no=i.xpath('td[2]/text()').extract_first()
            if 'Permit-' in permit_lic_no:
                permit_lic_no=permit_lic_no.replace('Permit-','')
            else:
                permit_lic_no=permit_lic_no

            loc1=i.xpath('td[3]/text()').extract_first()
            loc2=i.xpath('td[4]/text()').extract_first()
            location_address_string=loc1+' '+loc2+', SD'
            permit_subtype=i.xpath('td[5]/text()').extract_first()
            permit_lic_desc=i.xpath('td[6]/text()').extract_first().strip()
            if permit_lic_desc=='' or permit_lic_desc==None or len(permit_lic_desc)<1:
                permit_lic_desc=permit_subtype

            mixed_contractor=i.xpath('td[7]/text()').extract_first()

            if ',' in mixed_contractor:
                mixed_contractor_name=mixed_contractor.split(',')[0]
                contractor_lic_no=mixed_contractor.split(',')[1]
            else:
                mixed_contractor_name=mixed_contractor
                contractor_lic_no=''

            permit_lic_value=i.xpath('td[8]/text()').extract_first()
            if permit_lic_value=='0.00':
                permit_lic_value=''
            elif permit_lic_value:
                permit_lic_value='$'+permit_lic_value

            il = ItemLoader(item=SdSiouxfallsMinnehahaBuildingPermitsSpiderItem(),response=response)
            il.default_input_processor = MapCompose(lambda v: v.strip(), remove_tags, replace_escape_chars)
            #il.add_value('ingestion_timestamp', Utils.getingestion_timestamp())
            il.add_value('sourceName', 'SD_SiouxFalls_Minnehaha_Building_Permits')
            il.add_value('url', 'https://webapps.siouxfalls.org/permits/')
            il.add_value('contractor_lic_no',contractor_lic_no)
            il.add_value('permit_subtype',permit_subtype)
            il.add_value('location_address_string',location_address_string)
            il.add_value('permit_lic_eff_date', permit_lic_eff_date)
            il.add_value('permit_lic_value',permit_lic_value)
            il.add_value('mixed_contractor_name', self._getDBA(mixed_contractor_name)[0])
            il.add_value('contractor_dba',self._getDBA(mixed_contractor_name)[1])
            il.add_value('permit_lic_desc',permit_lic_desc)
            il.add_value('permit_type', 'building_permit')
            il.add_value('permit_lic_no', permit_lic_no)
            yield il.load_item()

        page_navi=response.xpath('//tr/td/table//tr/td/span[contains(text(),'+str(self.page)+')]/following::td/a/@href').extract_first()
        print("--------------------------------",page_navi)

        if page_navi:
            page_link=JavaScriptUtils.getValuesFromdoPost(page_navi)

            __VIEWSTATE=response.xpath('//*[@id="__VIEWSTATE"]/@value').extract_first()
            __VIEWSTATEGENERATOR=response.xpath('//*[@id="__VIEWSTATEGENERATOR"]/@value').extract_first()
            __EVENTVALIDATION=response.xpath('//*[@id="__EVENTVALIDATION"]/@value').extract_first()

            print('ssssssssssssssssssssssssssssssss',page_link)
            form_data={
                '__EVENTTARGET': page_link['__EVENTTARGET'],
                '__EVENTARGUMENT': page_link['__EVENTARGUMENT'],
                '__VIEWSTATE': __VIEWSTATE,
                '__VIEWSTATEGENERATOR': __VIEWSTATEGENERATOR,
                '__EVENTVALIDATION': __EVENTVALIDATION,
                'ctl00$ContentPlaceHolder1$ddlPermitType': '',
                'ctl00$ContentPlaceHolder1$txtHouseNum': '',
                'ctl00$ContentPlaceHolder1$txtStreetName':'', 
                'ctl00$ContentPlaceHolder1$ddlMonth': '',
                'ctl00$ContentPlaceHolder1$ddlYear': '2019',
            }
            yield scrapy.FormRequest(url=response.url, method='POST', formdata=form_data, callback =self.parse,  dont_filter=True)


        self.page=self.page+1


        # self.state['items_count'] = self.state.get('items_count', 0) + 1