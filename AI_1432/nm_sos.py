# -*- coding: utf-8 -*-

'''
Created on 2019-Jun-25 07:55:02
TICKET NUMBER -AI_1432
@author: ait-python
'''

from scrapy.loader import ItemLoader
from scrapy.loader.processors import MapCompose
from w3lib.html import remove_tags, replace_escape_chars

from Data_scuff.spiders.AI_1432.items import NmSosSpiderItem
from Data_scuff.spiders.__common import CommonSpider,CustomSettings
from Data_scuff.utils.utils import Utils
from Data_scuff.utils.JavaScriptUtils import JavaScriptUtils
import scrapy
import re
from scrapy.shell import inspect_response
from Data_scuff.utils.searchCriteria import SearchCriteria


class NmSosSpider(CommonSpider):
    name = 'ai_1432_nm_sos'
    allowed_domains = ['state.nm.us']
    start_urls = ['https://portal.sos.state.nm.us/BFS/online/CorporationBusinessSearch']
    site_key='6LcTYiwUAAAAAFZCzelvolLT0OEXctYN31ZpniI-'
    custom_settings = {
        'FILE_NAME':Utils.getRundateFileName('AI-1432_SOS_NM_CurationReady'),
        'JIRA_ID':'AI_1432',
        'COOKIES_ENABLED':True,
        'COOKIES_DEBUG':True,
        'HTTPCACHE_ENABLED':False,
        'CONCURRENT_REQUESTS': 1,
        'DOWNLOAD_DELAY':0.5,
        # 'JOBDIR' : CustomSettings.getJobDirectory('nm_sos'),
        'TOP_HEADER':{                        'business purpose': 'Business Purpose',
                         'company_name': 'Entity Name',
                         'company_subtype': 'Entity Type',
                         'creation_date': 'Date of Incorporation in NM/Date of Organization',
                         'dba_name': 'DBA Name',
                         'domestic state': 'Domestic State',
                         'entity_id': 'Business ID',
                         'location_address_string': 'Mailing Address',
                         'mixed_name': 'Registered Agent/Officer/General Partner information name',
                         'mixed_subtype': 'Agent/Officer/Partner Information Contact Title',
                         'non_profit_indicator': '',
                         'period of duration': 'Period of Duration',
                         'permit_type': 'permit_type',
                         'person_address_string': 'Agent/Officer /General Partner information Address',
                         'status': 'Status'},
        'FIELDS_TO_EXPORT':[                        
                         'company_name',
                         'entity_id',
                         'dba_name',
                         'company_subtype',
                         'non_profit_indicator',
                         'location_address_string',
                         'status',
                         'creation_date',
                         'domestic state',
                         'period of duration',
                         'business purpose',
                         'mixed_subtype',
                         'mixed_name',
                         'person_address_string',
                         'permit_type',
                         'sourceName',
                         'url',
                         'ingestion_timestamp'],
                         
        'NULL_HEADERS':['domestic state', 'business purpose', 'period of duration']
        }
    
    def parse(self, response):
        self.searchkeys = []
        if self.start.isalpha() and self.end.isalpha():
            self.searchkeys = SearchCriteria.strRange(self.start,self.end)
        else:
            self.searchkeys = SearchCriteria.numberRange(self.start,self.end,1)
        search=self.searchkeys.pop(0)

        self.form_data={
            'search.SearchName': 'express',
            'search.SearchType': 'BusinessName',
            'search.BusinessName': search,
            'search.ActualBusinessName': search,
            'search.BusinessId': '',
            'search.SearchCriteria': 'StartsWith',
            'search.CitizenShipType':'' ,
            'search.BusinessTypeId': '0',
            'search.BusinessStatusId': '',
            'search.NaicsCode':'' ,
            'search.businessStatus':'', 
            'search.isGoodStanding': '',
            'search.Country': '',
            'search.Zip': '',
            'search.City': '',
            'search.State': '',
            'search.OtherState':'', 
            'search.PostalCode': '',
            'search.AgentType': '',
            'search.RAFirstName':'', 
            'search.RAMiddleName':'', 
            'search.RALastName': '',
            'search.RASuffix': '',
            'search.RAName': '',
            'search.RAAddress1':'', 
            'search.RAAddress2': '',
            'search.RACountry': '',
            'search.RAZip': '',
            'search.RACity': '',
            'search.RAState': '',
            'search.RAOtherState':'', 
            'search.RAPostalCode': '',
            'search.DirectorFirstName':'', 
            'search.DirectorMiddleName': '',
            'search.DirectorLastName': '',
            'search.DirectorSuffix': '',
            'search.IncorporatorType':'', 
            'search.IncorporatorFirstName':'', 
            'search.IncorporatorMiddleName':'', 
            'search.IncorporatorLastName': '',
            'search.IncorporatorSuffix': '',
            'search.IncorporatorEntityName':'', 
            'search.IncorporatorAddress1': '',
            'search.IncorporatorAddress2': '',
            'search.IncorporatorCountry': '',
            'search.IncorporatorZip': '',
            'search.IncorporatorCity': '',
            'search.IncorporatorState': '',
            'search.IncorporatorOtherState':'', 
            'search.IncorporatorPostalCode': '',
            'search.OrganizerFirstName': '',
            'search.OrganizerMiddleName': '',
            'search.OrganizerLastName': '',
            'search.OrganizerSuffix': '',
            'search.ReservationNo': '',
            'search.CaptchaResponse': self.getcaptchaCoder(self.site_key).resolver(response.url),
        }
        yield scrapy.FormRequest(response.url,formdata=self.form_data,dont_filter=True,method='POST', callback=self.parse_two, meta={'page':2})

    requestt = ''
    def parse_two(self,response):
        metaa ={}
        metaa=response.meta
        if metaa['page']==2:
            self.requestt=response.xpath("//input[@name='__RequestVerificationToken']/@value").extract_first()

        main_tbl=response.xpath('//*[@id="xhtml_Businessesgrid"]//tr')[1:]
        for i in main_tbl:
            name_link=i.xpath('td[1]/a/@onclick').extract_first()
            metaa['id_number']=re.search(r'\d+',str(name_link)).group()
            form_data={

                'txtCommonPageNo':'', 
                'hdnTotalPgCount': '129',
                'txtCommonPageNo': '',
                'hdnTotalPgCount': '2',
                'businessId':str(metaa['id_number']),
                '__RequestVerificationToken': self.requestt,
            }
            yield scrapy.FormRequest(url='https://portal.sos.state.nm.us/BFS/online/CorporationBusinessSearch/CorporationBusinessInformation',formdata=form_data, callback=self.page_three, meta={'page':response.meta['page'],'meta':metaa['id_number']} )

        page=response.xpath("//a[contains(@href,'xhtmlCorp.paging')][text()='Next >']/@href").extract()     
        if page:
            formdata1={
                 'undefined':'',
                 'sortby': '',
                 'stype': 'a',
                 'pidx':str(response.meta['page'])
                 }

            header={
                'Accept': '*/*',
                'Host': 'portal.sos.state.nm.us',
                'Origin': 'https://portal.sos.state.nm.us',
                'Referer': 'https://portal.sos.state.nm.us/BFS/online/CorporationBusinessSearch',
                'User-Agent': 'Mozilla/5.0 (X11; Linux x86_64)AppleWebKit/537.36 (KHTML, like Gecko) Chrome/72.0.3626.121 Safari/537.36',
                'X-Requested-With': 'XMLHttpRequest',
                '__RequestVerificationToken':self.requestt
                }
            yield scrapy.FormRequest(url='https://portal.sos.state.nm.us/BFS/online/CorporationBusinessSearch/BusinessList',headers=header,method='POST',dont_filter=True,formdata=formdata1,callback=self.parse_two,meta={'page':response.meta['page']+1,'metaa':metaa})
        else:
            search=self.searchkeys.pop(0)
            self.form_data['search.BusinessName']=search
            self.form_data['search.ActualBusinessName']=search
            yield scrapy.FormRequest(response.url,formdata=self.form_data,dont_filter=True,method='POST', callback=self.parse_two, meta={'page':2})


    def page_three(self,response):
        print('------------------ inside::')
        meta=response.meta
        meta['company_name']=response.xpath("//td[contains(text(),'Entity Name:')]/following-sibling::td[1]/strong/text()").extract_first()
        meta['business_id']=response.xpath("//td[contains(text(),'Business ID#:')]/following-sibling::td[1]/strong/text()").extract_first()
        meta['dba_name']=response.xpath("//td[contains(text(),'DBA Name:')]/following-sibling::td[1]/strong/text()").extract_first()
        if meta['dba_name'] =='Not Applicable':
            meta['dba_name']=''
        meta['company_subtype']=response.xpath("//td[contains(text(),'Entity Type:')]/following-sibling::td[1]/strong/text()").extract_first()

        if 'non profit' in meta['company_subtype'] or 'not profit' in meta['company_subtype'] or 'Nonprofit' in meta['company_subtype'] or 'Notprofit' in meta['company_subtype']:
            meta['non_profit_indicator']='Yes'
        else:
            meta['non_profit_indicator']=''

        meta['location_address_string']=response.xpath("//span[contains(text(),'Mailing Address:')]/ancestor::td/following-sibling::td/strong/text()").extract_first()

        meta['location_address_string']=meta['location_address_string'] if meta['location_address_string'] else 'NM'

        meta['status']=response.xpath("//td[contains(text(),'Status:')]/following-sibling::td[1]/strong/text()").extract_first()
        meta['creation_date']=response.xpath("//td[contains(text(),'Date of Appointment:')]/following-sibling::td[1]/strong/text()").extract_first()
        meta['domestic_state']=response.xpath("//td[contains(text(),'State of Incorporation:')]/following-sibling::td[1]/strong/text()").extract_first()
        meta['peroid_of_duration']=response.xpath("//td[contains(text(),'Period of Duration:')]/following-sibling::td[1]/strong/text()").extract_first()
        meta['business_purpose']=response.xpath("//td[contains(text(),'Business Purpose:')]/following-sibling::td[1]/strong/text()").extract_first()

        office_info=response.xpath("//table[@id='grid_OfficersList']//tr")[1:]
        for tr in office_info:
            meta['officer_title']=tr.xpath('.//td[1]/text()').extract_first()
            meta['officer_name']=tr.xpath('.//td[2]/text()').extract_first()
            meta['officer_address']=tr.xpath('.//td[3]/text()').extract_first()
            if meta['officer_address'] == 'NONE'or meta['officer_address']=='':
                meta['officer_address']='NM'
            if 'No Records to View' not in meta['officer_title']:
                yield self.save_to_csv(response,**meta).load_item()

        director_info=response.xpath('//*[@id="grid_DirectorList"]//tr')[1:]
        for dr in director_info:
            meta['officer_title']=dr.xpath('.//td[1]/text()').extract_first()
            meta['officer_name']=dr.xpath('.//td[2]/text()').extract_first()
            meta['officer_address']=tr.xpath('.//td[3]/text()').extract_first()
            if meta['officer_address'] == 'NONE'or meta['officer_address']=='':
                meta['officer_address']='NM'
            yield self.save_to_csv(response,**meta).load_item()

        agent_name=response.xpath("//td[starts-with(text(),'Name:')]/following-sibling::td[1]/strong/text()").extract_first()
        meta['officer_name']=agent_name
        agent_address=response.xpath("//td[starts-with(text(),'Physical Address:')]/following-sibling::td[1]/strong/text()").extract_first()
        meta['officer_address']=agent_address
        agent_subtype='Agent'
        meta['officer_title']=agent_subtype
        yield self.save_to_csv(response,**meta).load_item()


        

    def save_to_csv(self,response, **meta):
        # self.state['items_count'] = self.state.get('items_count', 0) + 1
        il = ItemLoader(item=NmSosSpiderItem(),response=response)
        il.default_input_processor = MapCompose(lambda v: v.strip(), remove_tags, replace_escape_chars)
        #il.add_value('ingestion_timestamp', Utils.getingestion_timestamp())       
        il.add_value('company_name',meta['company_name'])
        il.add_value('entity_id',meta['business_id'])
        il.add_value('dba_name', meta['dba_name'])
        il.add_value('company_subtype',meta['company_subtype'])
        il.add_value('non_profit_indicator',meta['non_profit_indicator'])
        il.add_value('location_address_string',meta['location_address_string'])
        il.add_value('status',meta['status'])
        il.add_value('creation_date',meta['creation_date'])
        il.add_value('domestic state',meta['domestic_state'])
        il.add_value('period of duration',meta['peroid_of_duration'])
        il.add_value('business purpose',meta['business_purpose'])
        il.add_value('mixed_subtype', meta['officer_title'])
        il.add_value('mixed_name', meta['officer_name'])
        il.add_value('person_address_string',meta['officer_address'])
        il.add_value('permit_type', 'business_license')
        il.add_value('sourceName', 'NM_SOS')
        il.add_value('url', 'https://portal.sos.state.nm.us/BFS/online/CorporationBusinessSearch')
        return il