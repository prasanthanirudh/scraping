# -*- coding: utf-8 -*-

'''
Created on 2019-Aug-05 03:33:43
TICKET NUMBER -AI_1498
@author: ait-python
'''
from scrapy.http import HtmlResponse
from scrapy.loader import ItemLoader
from scrapy.loader.processors import MapCompose
from w3lib.html import remove_tags, replace_escape_chars

from Data_scuff.spiders.AI_1498.items import MsElectronicProtectionLicensesSpiderItem
from Data_scuff.spiders.__common import CommonSpider,CustomSettings
from Data_scuff.utils.utils import Utils
from scrapy.shell import inspect_response
import scrapy
from Data_scuff.utils.JavaScriptUtils import JavaScriptUtils


class MsElectronicProtectionLicensesSpider(CommonSpider):
    name = '1498_ms_electronic_protection_licenses'
    allowed_domains = ['ms.gov']
    start_urls = ['https://www.mid.ms.gov/sfm/mississippi-electronic-protection-systems.aspx#Licensing%2520Search']
    
    custom_settings = {
        'FILE_NAME':Utils.getRundateFileName('AI-1498_Licenses_Electronic_Protection_MS_CurationReady'),
        'JIRA_ID':'AI_1498',
        'HTTPCACHE_ENABLED':False,
        'COOKIES_ENABLED':True,
        'COOKIES_DEBUG':True,
        # 'JOBDIR' : CustomSettings.getJobDirectory('ms_electronic_protection_licenses'),
        'TOP_HEADER':{                        
                         ' nat. prod. id': 'Nat. Prod. ID',
                         'company_name': 'Name',
                         'company_phone': 'Phone',
                         'dba_name': '',
                         'location_address_string': 'Physical Address',
                         'permit_lic_desc': '',
                         'permit_lic_exp': 'Exp. Date',
                         'permit_lic_no': 'License Number',
                         'permit_subtype': 'Type',
                         'permit_type': '',
                         'person_name': 'First+Middle+Last name'},
        'FIELDS_TO_EXPORT':[
                                'permit_subtype', 
                                'permit_lic_no', 
                                'company_name', 
                                'dba_name', 
                                'person_name', 
                                ' nat. prod. id', 
                                'location_address_string', 
                                'company_phone',
                                'permit_lic_exp', 
                                'permit_lic_desc', 
                                'permit_type', 
                                'url', 
                                'ingestion_timestamp', 
                                'sourceName'], 
        'NULL_HEADERS':['nat. prod. id']
        }
    def parse(self, response):
    


        table_data=response.xpath('//*[@id="maincontent_pagecontent_rbls"]//tr/td//text()').extract()
        tb_list=['Class A - Contracting Company','REPT','REPH','REPC','REPD','REPB']

        for i,j in zip(table_data,tb_list):
            form_data={
                'ctl00$ctl00$maincontent$pagecontent$ToolkitScriptManager1': 'ctl00$ctl00$maincontent$pagecontent$UpdatePanel1|ctl00$ctl00$maincontent$pagecontent$ButtonSubmit',
                '_TSM_HiddenField_':response.xpath("//input[@id='_TSM_HiddenField_']/@value").extract_first(),
                'ctl00$ctl00$maincontent$pagecontent$rbls':str(j),
                '__EVENTTARGET':'', 
                '__EVENTARGUMENT': '',
                '__LASTFOCUS':'',
                '__VIEWSTATE': response.xpath("//input[@id='__VIEWSTATE']/@value").extract_first(),
                '__VIEWSTATEGENERATOR': response.xpath("//input[@id='__VIEWSTATEGENERATOR']/@value").extract_first(),
                '__EVENTVALIDATION': response.xpath("//input[@id='__EVENTVALIDATION']/@value").extract_first(),
                '__ASYNCPOST': 'true',
                'ctl00$ctl00$maincontent$pagecontent$ButtonSubmit': 'Submit',
            }

            # print("===================================",str(j),str(i))
            yield scrapy.FormRequest(response.url,callback=self.detail_page,dont_filter=True,method='POST', formdata=form_data,meta={'page':1,'permit_lic_desc':str(i),'option':str(j)})

    
    def detail_page(self,response):
        meta={}
        meta=response.meta        
        res=''
        meta['permit_lic_desc']=meta['permit_lic_no']=meta['company_name']=meta['person_name']=meta['nat_prof_id']=meta['location_address_string']=meta['phone']=meta['exp_date']=meta['permit_lic_desc']=meta['dba_name']=''
        if isinstance(response,HtmlResponse):
            res=response
        else:
            res = HtmlResponse('https://attorneyinfo.aoc.arkansas.gov/info/attorney_search/info/attorney/attorneysearch.aspx?', body=str.encode(response.text))
            viewstate = response.text.split('__VIEWSTATE|')[1].split('|')[0]
            __VIEWSTATEGENERATOR = response.text.split('__VIEWSTATEGENERATOR|')[1].split('|')[0]
            tsm='2GFwlGU9ATlFIxrdsXRzcja58_1t5F8HSleaZM4ZQwk1'
            __EVENTVALIDATION=response.text.split('__EVENTVALIDATION|')[1].split('|')[0]

        # print("============================================================",meta['option'])
        if meta['option']=='Class A - Contracting Company':
            table_data=res.xpath('//*[@id="maincontent_pagecontent_DataGrid4"]//tr')
            for i in table_data[1:-1]:
                meta['permit_lic_no']=i.xpath('td[1]/text()').extract_first()
                company_name=i.xpath('td[2]/text()').extract_first()
                meta['company_name']=self._getDBA(company_name)[0]
                meta['dba_name']=self._getDBA(company_name)[1]
                add1=i.xpath('td[3]/text()').extract_first()
                add2=i.xpath('td[4]/text()').extract_first()
                add3=i.xpath('td[5]/text()').extract_first()
                city=i.xpath('td[6]/text()').extract_first()
                state=i.xpath('td[7]/text()').extract_first()
                code=i.xpath('td[8]/text()').extract_first()

                print("------------------------------",meta['company_name'])
                meta['location_address_string']=self.format__address6(add1,add2,add3,city,state,code).replace(' ,','')
                meta['phone']=i.xpath('td[9]/text()').extract_first()
                meta['permit_lic_desc']='Company'
                yield self.save_to_csv(response,**meta).load_item()

            next_page=res.xpath('//*[@id="maincontent_pagecontent_DataGrid4"]//tr/td/span[contains(text(), "'+str(meta['page'])+'")]/following::a/@href').extract_first()                
        
            if next_page:
                datas = JavaScriptUtils.getValuesFromdoPost(next_page)
                form_data={
                'ctl00$ctl00$maincontent$pagecontent$ToolkitScriptManager1':'ctl00$ctl00$maincontent$pagecontent$UpdatePanel1|'+datas['__EVENTTARGET'],
                '_TSM_HiddenField_':tsm,
                'ctl00$ctl00$maincontent$pagecontent$rbls':'Class A - Contracting Company',
                '__EVENTTARGET': datas['__EVENTTARGET'],
                '__EVENTARGUMENT':datas['__EVENTARGUMENT'],
                '__LASTFOCUS': '',
                '__VIEWSTATE':viewstate,
                '__VIEWSTATEGENERATOR': __VIEWSTATEGENERATOR,
                '__EVENTVALIDATION': __EVENTVALIDATION,
                '__ASYNCPOST': 'true'
                }
                yield scrapy.FormRequest(response.url,callback=self.detail_page,dont_filter=True,method='POST', formdata=form_data,meta={'page':meta['page']+1,'option':meta['option']})

        if meta['option']=='REPH' or  meta['option']=='REPC' or  meta['option']=='REPD' or  meta['option']=='REPB':

            table_data=res.xpath('//*[@id="maincontent_pagecontent_DataGrid1"]//tr')
            for i in table_data[1:-1]:
                meta['permit_lic_no']=i.xpath('td[1]/a/text()').extract_first()
                meta['nat_prof_id']=i.xpath('td[2]/text()').extract_first()
                f_name=i.xpath('td[3]/text()').extract_first()
                l_name=i.xpath('td[4]/text()').extract_first()
                m_name=i.xpath('td[5]/text()').extract_first()
                s_name=i.xpath('td[6]/text()').extract_first()
                meta['person_name']=f_name+' '+m_name+' '+l_name+' '+s_name
                add1=i.xpath('td[7]/text()').extract_first()
                add2=i.xpath('td[8]/text()').extract_first()
                city=i.xpath('td[9]/text()').extract_first()
                state=i.xpath('td[10]/text()').extract_first()
                code=i.xpath('td[11]/text()').extract_first()
                meta['location_address_string']=self.format__address5(add1,add2,city,state,code)
                meta['phone']=i.xpath('td[12]/text()').extract_first()
                meta['exp_date']=i.xpath('td[13]/text()').extract_first()
                meta['company_name']=meta['person_name']
                if meta['option']=='REPH':
                    meta['permit_lic_desc']='Helper'
                if meta['option']=='REPC':
                    meta['permit_lic_desc']='Installer'
                if meta['option']=='REPD':
                    meta['permit_lic_desc']='Salesperson'
                if meta['option']=='REPB':
                    meta['permit_lic_desc']='Technician'
                yield self.save_to_csv(response,**meta).load_item()                

            next_page=res.xpath('//*[@id="maincontent_pagecontent_DataGrid1"]//tr/td/span[contains(text(), "'+str(meta['page'])+'")]/following::a/@href').extract_first()                
            if next_page:
                datas = JavaScriptUtils.getValuesFromdoPost(next_page)
                form_data={
                'ctl00$ctl00$maincontent$pagecontent$ToolkitScriptManager1':'ctl00$ctl00$maincontent$pagecontent$UpdatePanel1|'+datas['__EVENTTARGET'],
                '_TSM_HiddenField_':tsm,
                'ctl00$ctl00$maincontent$pagecontent$rbls':meta['option'],
                '__EVENTTARGET': datas['__EVENTTARGET'],
                '__EVENTARGUMENT':datas['__EVENTARGUMENT'],
                '__LASTFOCUS': '',
                '__VIEWSTATE':viewstate,
                '__VIEWSTATEGENERATOR': __VIEWSTATEGENERATOR,
                '__EVENTVALIDATION': __EVENTVALIDATION,
                '__ASYNCPOST': 'true'
                }
                yield scrapy.FormRequest(response.url,callback=self.detail_page,dont_filter=True,method='POST', formdata=form_data,meta={'page':meta['page']+1,'option':meta['option']})

    def save_to_csv(self,response,**meta):
        # self.state['items_count'] = self.state.get('items_count', 0) + 1
        il = ItemLoader(item=MsElectronicProtectionLicensesSpiderItem(),response=response)
        # il.default_input_processor = MapCompose(lambda v: v.strip(), remove_tags, replace_escape_chars)
        #il.add_value('ingestion_timestamp', Utils.getingestion_timestamp())
        il.add_value('url', 'https://www.mid.ms.gov/sfm/mississippi-electronic-protection-systems.aspx#Licensing%2520Search')
        il.add_value('sourceName', 'MS_Electronic_Protection_Licenses')
        il.add_value('permit_subtype',meta['permit_lic_desc'])
        il.add_value('permit_lic_no',meta['permit_lic_no'])
        il.add_value('company_name',meta['company_name'])
        il.add_value('dba_name',meta['dba_name'])
        il.add_value('person_name',meta['person_name'])
        il.add_value(' nat. prod. id',meta['nat_prof_id'])
        il.add_value('location_address_string',meta['location_address_string'])
        il.add_value('company_phone',meta['phone'])
        il.add_value('permit_lic_exp',meta['exp_date'])
        il.add_value('permit_lic_desc',meta['permit_lic_desc'])
        il.add_value('permit_type', 'electronics_license')
        return il