# -*- coding: utf-8 -*-

'''
Created on 2019-Aug-26 09:33:10
TICKET NUMBER -AI_1738
@author: ait-python
'''

from scrapy.loader import ItemLoader
from scrapy.loader.processors import MapCompose
from w3lib.html import remove_tags, replace_escape_chars

from Data_scuff.spiders.AI_1738.items import SdSeptictankLicensesSpiderItem
from Data_scuff.spiders.__common import CommonSpider,CustomSettings
from Data_scuff.utils.utils import Utils
from scrapy.shell import inspect_response
import scrapy
import re
import json
import time
from time import time
from scrapy.http import HtmlResponse

class SdSeptictankLicensesSpider(CommonSpider):
    name = '1738_sd_septictank_licenses'
    allowed_domains = ['sd.gov']
    start_urls = ['https://c0bkr159.caspio.com/dp/31cf1000eccbd58b888d45ff8350']
    
    custom_settings = {
        'FILE_NAME':Utils.getRundateFileName('AI-1738_Licenses_SepticTank_SD_CurationReady'),
        'JIRA_ID':'AI_1738',
        'HTTPCACHE_ENABLED':False,
        'COOKIES_ENABLED':True,
        'COOKIES_DEBUG':True,
        'DOWNLOAD_DELAY':10,
        # 'CONCURRENT_REQUESTS':1,
        # 'JOBDIR' : CustomSettings.getJobDirectory('sd_septictank_licenses'),
        'TOP_HEADER':{                        'company_name': 'Business',
                         'company_phone': 'Phone',
                         'county': 'County',
                         'dba_name': '',
                         'location_address_string': 'Address + City + State + Zip',
                         'permit_lic_desc': '',
                         'permit_lic_eff_date': 'Date Certified',
                         'permit_lic_exp_date': 'Date of Expiration',
                         'permit_type': '',
                         'person_name': 'Fname + Lname'},
        'FIELDS_TO_EXPORT':[
                        'company_name', 
                        'dba_name', 
                        'person_name', 
                        'company_phone', 
                        'county', 
                        'location_address_string',
                        'City',
                        'State',
                        'Zip', 
                        'permit_lic_eff_date', 
                        'permit_lic_exp_date', 
                        'permit_lic_desc', 
                        'permit_type', 
                        'sourceName', 
                        'url',
                        'ingestion_timestamp',],
        'NULL_HEADERS':['county']
        }

    def parse(self, response):
        meta={}
        a =int(time()*1000)
        url=response.url+'?rnd='+str(a)
        ComparisonType1_1= response.xpath('//*[@name="ComparisonType1_1"]/@value').extract_first()
        cbUniqueFormId=response.xpath('//*[@name="cbUniqueFormId"]/@value').extract_first()
        MatchNull1_1=response.xpath('//*[@name="MatchNull1_1"]/@value').extract_first()
        FieldName2=response.xpath('//*[@name="FieldName2"]/@value').extract_first()
        Operator2=response.xpath('//*[@name="Operator2"]/@value').extract_first()
        NumCriteriaDetails2=response.xpath('//*[@name="NumCriteriaDetails2"]/@value').extract_first()
        ComparisonType2_1=response.xpath('//*[@name="ComparisonType2_1"]/@value').extract_first()
        MatchNull2_1=response.xpath('//*[@name="MatchNull2_1"]/@value').extract_first()
        FieldName3=response.xpath('//*[@name="FieldName3"]/@value').extract_first()
        Operator3=response.xpath('//*[@name="Operator3"]/@value').extract_first()
        NumCriteriaDetails3=response.xpath('//*[@name="NumCriteriaDetails3"]/@value').extract_first()
        ComparisonType3_1=response.xpath('//*[@name="ComparisonType3_1"]/@value').extract_first()
        MatchNull3_1=response.xpath('//*[@name="MatchNull3_1"]/@value').extract_first()
        FieldName4=response.xpath('//*[@name="FieldName4"]/@value').extract_first()
        Operator4=response.xpath('//*[@name="Operator4"]/@value').extract_first()
        NumCriteriaDetails4=response.xpath('//*[@name="NumCriteriaDetails4"]/@value').extract_first()
        ComparisonType4_1=response.xpath('//*[@name="ComparisonType4_1"]/@value').extract_first()
        MatchNull4_1=response.xpath('//*[@name="MatchNull4_1"]/@value').extract_first()
        FieldName5=response.xpath('//*[@name="FieldName5"]/@value').extract_first()
        Operator5=response.xpath('//*[@name="Operator5"]/@value').extract_first()
        NumCriteriaDetails5=response.xpath('//*[@name="NumCriteriaDetails5"]/@value').extract_first()
        ComparisonType5_1=response.xpath('//*[@name="ComparisonType5_1"]/@value').extract_first()
        MatchNull5_1=response.xpath('//*[@name="MatchNull5_1"]/@value').extract_first()
        FieldName6=response.xpath('//*[@name="FieldName6"]/@value').extract_first()
        Operator6=response.xpath('//*[@name="Operator6"]/@value').extract_first()
        NumCriteriaDetails6=response.xpath('//*[@name="NumCriteriaDetails6"]/@value').extract_first()
        ComparisonType6_1=response.xpath('//*[@name="ComparisonType6_1"]/@value').extract_first()
        MatchNull6_1=response.xpath('//*[@name="MatchNull6_1"]/@value').extract_first()
        AppKey=response.xpath('//*[@name="AppKey"]/@value').extract_first()
        PrevPageID=response.xpath('//*[@name="PrevPageID"]/@value').extract_first()
        PageID=response.xpath('//*[@name="PageID"]/@value').extract_first()
        cbPageType=response.xpath('//*[@name="cbPageType"]/@value').extract_first()
        GlobalOperator=response.xpath('//*[@name="GlobalOperator"]/@value').extract_first()
        NumCriteria=response.xpath('//*[@name="NumCriteria"]/@value').extract_first()
        Search=response.xpath('//*[@name="Search"]/@value').extract_first()
        cbSpaInitialSearch=response.xpath('//*[@name="cbSpaInitialSearch"]/@value').extract_first()
        cbSearchResultsUniqueId=response.xpath('//*[@name="cbUniqueFormId"]/@value').extract()
        cc=response.xpath('/html//script[1]/text()').extract_first()
        aa = cc[cc.rfind('spaReportUniqueSuffix')+1 : ].split(',')[0]
        unqiue=re.search(r'_[0-9].*',aa)
        unique=unqiue.group()
        AjaxAction='SearchForm'
        GridMode='False'
        cbUniqueFormId=response.xpath('//*[@name="cbUniqueFormId"]/@value').extract_first()
        AjaxActionHostName='https://c0bkr159.caspio.com'
        form_data={
            'Value2_1': '',
            'Value3_1': '',
            'Value4_1': '',
            'Value5_1': '',
            'Value6_1': '',
            'cbUniqueFormId': cbUniqueFormId,
            'ComparisonType1_1': ComparisonType1_1,
            'MatchNull1_1': MatchNull1_1,
            'FieldName2': FieldName2,
            'Operator2': Operator2,
            'NumCriteriaDetails2':NumCriteriaDetails2,
            'ComparisonType2_1':ComparisonType2_1,
            'MatchNull2_1': MatchNull2_1,
            'FieldName3': FieldName3,
            'Operator3': Operator3,
            'NumCriteriaDetails3': NumCriteriaDetails3,
            'ComparisonType3_1': ComparisonType3_1,
            'MatchNull3_1': MatchNull3_1,
            'FieldName4': FieldName4,
            'Operator4': Operator4,
            'NumCriteriaDetails4': NumCriteriaDetails4,
            'ComparisonType4_1': ComparisonType4_1,
            'MatchNull4_1': MatchNull4_1,
            'FieldName5': FieldName5,
            'Operator5': Operator5,
            'NumCriteriaDetails5': NumCriteriaDetails5,
            'ComparisonType5_1': ComparisonType5_1,
            'MatchNull5_1': MatchNull5_1,
            'FieldName6': FieldName6,
            'Operator6': Operator6,
            'NumCriteriaDetails6': NumCriteriaDetails6,
            'ComparisonType6_1':ComparisonType6_1,
            'MatchNull6_1':MatchNull6_1,
            'AppKey': AppKey,
            'PrevPageID': PrevPageID,
            'cbPageType': 'Search',
            'PageID': PageID,
            'GlobalOperator':GlobalOperator,
            'NumCriteria':NumCriteria,
            'Search': Search,
            'cbSpaInitialSearch':'True',
            'cbSearchResultsUniqueId': unique,
            'AjaxAction': 'SearchForm',
            'GridMode': 'False',
            'cbUniqueFormId': cbUniqueFormId,
            'AjaxActionHostName': 'https://c0bkr159.caspio.com',

        }
        headerr = {
        'Origin': 'https://c0bkr159.caspio.com',
        'Referer': 'https://c0bkr159.caspio.com/dp/31cf1000eccbd58b888d45ff8350',
        'Sec-Fetch-Mode': 'cors',
        'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_14_6) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/76.0.3809.132 Safari/537.36',
        }
        yield scrapy.FormRequest(url=url,method='POST',formdata=form_data,callback=self.on_detail_page,headers=headerr, dont_filter=True,meta={'cbSearchResultsUniqueId':cbUniqueFormId})

    def on_detail_page(self,response):
        meta=response.meta

        ccc=json.loads(response.text)['reportSettings']['responseText']
        __response = HtmlResponse(response.url, body=str.encode(ccc))
        appSession =ccc[ccc.rfind("appSession")+13 :ccc.rfind('","uniqueId')]
        appSession='appSession='+appSession+'&PageID=2'
        link=__response.xpath("//table[@data-cb-name='cbTable']//tr[@data-cb-name='data'][1]//td[8]/a/@href").extract_first()
        app =link[link.rfind('?appSession=')+12 :link.rfind('&PageID=')]
        cbUniqueFormId=response.xpath('//*[@name="cbUniqueFormId"]/@value').extract_first()

        ClientQueryString=appSession
        PageID=link[link.rfind('&PageID=')+8 :link.rfind('&PrevPageID')]
        PrevPageID=link[link.rfind('&PrevPageID=')+12 :link.rfind('&cpipage=')]
        cpipage=link[link.rfind('&cpipage=')+1:link.rfind('&RecordID')]
        RecordID=link[link.rfind('&RecordID=')+10:link.rfind('&cbCurrentRe')]
        cbCurrentRecordPosition=link[link.rfind('&cbCurrentRecordPosition=')+25:link.rfind('&Mod0LinkToDetails=')]
        Mod0LinkToDetails=link[link.rfind('Mod0LinkToDetails')+18:link.rfind('&cbRandomSortKey')]
        cbRandomSortKey=link[link.rfind('&cbRandomSortKey')+17:link.rfind('&cbCurrentPageSize=')]
        cbCurrentPageSize=link[link.rfind('&cbCurrentPageSize')+19:]
        form_data={

                'AjaxAction': 'ViewDetails',
                'GridMode': 'False',
                'cbUniqueFormId': response.meta['cbSearchResultsUniqueId'],
                'ClientQueryString':'', 
                'appSession':app,
                'PageID':PageID,
                'PrevPageID': PrevPageID,
                'cpipage': '1',
                'RecordID':RecordID,
                'cbCurrentRecordPosition': cbCurrentRecordPosition,
                'Mod0LinkToDetails':Mod0LinkToDetails,
                'cbRandomSortKey':cbRandomSortKey,
                'cbCurrentPageSize':cbCurrentPageSize,
                'AjaxActionHostName': 'https://c0bkr159.caspio.com'
            }
        headerr = {
        'Origin': 'https://c0bkr159.caspio.com',
        'Referer': 'https://c0bkr159.caspio.com/dp/31cf1000eccbd58b888d45ff8350',
        'Sec-Fetch-Mode': 'cors',
        'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_14_6) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/76.0.3809.132 Safari/537.36',
        }

        yield scrapy.FormRequest(url=response.url, callback = self.parse_three,method='POST',dont_filter=True, formdata=form_data,meta=meta,headers=headerr)

    def parse_three(self,response):
        meta=response.meta
        unqiue_form=meta['cbSearchResultsUniqueId']
        bbb=json.loads(response.text)['responseText']
        __response = HtmlResponse(response.url, body=str.encode(bbb))
        business_name=__response.xpath("//td[starts-with(text(),'Business:')]/following::tr/td[1]/span/text()").extract_first()
        if not business_name:
            inspect_response(__response, self)
        f_name=__response.xpath("//td[starts-with(text(),'FName:')]/following::tr/td[1]/span/text()").extract_first()
        l_name=__response.xpath("//td[starts-with(text(),'LName')]/following::tr/td[1]/span/text()").extract_first()
        phone=__response.xpath("//td[starts-with(text(),'Phone:')]/following::tr/td[1]/span/text()").extract_first()
        county=__response.xpath("//td[starts-with(text(),'County:')]/following::tr/td[1]/span/text()").extract_first()
        add=__response.xpath("//td[starts-with(text(),'Address:')]/following::tr/td[1]/span/text()").extract_first()
        city=__response.xpath("//td[starts-with(text(),'City:')]/following::tr/td[1]/span/text()").extract_first()
        state=__response.xpath("//td[starts-with(text(),'State:')]/following::tr/td[1]/span/text()").extract_first()
        zip_code=__response.xpath("//td[starts-with(text(),'Zip:')]/following::tr/td[1]/span/text()").extract_first()
        print("-------------------0999999999999",business_name,f_name,l_name,phone,county,add,city,state,zip_code)
        date_certified=__response.xpath("//td[starts-with(text(),'Date Certified:')]/following::tr/td[1]/span/text()").extract_first()
        expire_date=__response.xpath("//td[starts-with(text(),'Date of Expiration:')]/following::tr/td[1]/span/text()").extract_first()
        person_name=f_name+' '+l_name
        location_address_string=self.format__address_4(add,city,state,zip_code)

        next_page=__response.xpath('//a[@data-cb-name="JumpToNext"]/@href').extract_first()
        if next_page:

            app =next_page[next_page.rfind('?appSession=')+12:next_page.rfind('&RecordID=')]
            RecordID=next_page[next_page.rfind('&RecordID=')+10:next_page.rfind('&cpipage=')]
            cpipage=next_page[next_page.rfind('&cpipage=')+9:next_page.rfind('&PageID')]
            PageID=next_page[next_page.rfind('&PageID=')+8 :next_page.rfind('&PrevPageID')]
            PrevPageID=next_page[next_page.rfind('&PrevPageID=')+12 :next_page.rfind('&CPISortType=')]
            cbCurrentPageSize=next_page[next_page.rfind('&cbCurrentPageSize')+19:next_page.rfind('&cbRandomSortKey=')]
            cbRandomSortKey=next_page[next_page.rfind('&cbRandomSortKey')+17:next_page.rfind('&cbRecordPosition')]
            cbCurrentRecordPosition=next_page[next_page.rfind('&cbRecordPosition=')+18:]

            a =int(time()*1000)
            url1='https://c0bkr159.caspio.com/dp/31cf1000eccbd58b888d45ff8350?rnd='+str(a)

            print("---form------------------------------>",url1)
            form_data={

                'AjaxAction': 'JumpToNext',
                'GridMode': 'False',
                'cbUniqueFormId': unqiue_form,
                'ClientQueryString':'', 
                'appSession': app,
                'RecordID': RecordID,
                'cpipage': cpipage,
                'PageID': PageID,
                'PrevPageID': PrevPageID,
                'CPISortType': '',
                'CPIorderBy': '',
                'cbCurrentPageSize':cbCurrentPageSize,
                'cbRandomSortKey': cbRandomSortKey,
                'cbRecordPosition': cbCurrentRecordPosition,
                'AjaxActionHostName': 'https://c0bkr159.caspio.com',

            }
            print("---form------------------------------>",form_data)

            headerr = {
            'Origin': 'https://c0bkr159.caspio.com',
            'Referer': 'https://c0bkr159.caspio.com/dp/31cf1000eccbd58b888d45ff8350',
            'Sec-Fetch-Mode': 'cors',
            'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_14_6) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/76.0.3809.132 Safari/537.36',
            }

            yield scrapy.FormRequest(url=url1, callback = self.parse_three,method='POST',dont_filter=True, formdata=form_data,meta=meta,headers=headerr)

        il = ItemLoader(item=SdSeptictankLicensesSpiderItem(),response=response)
        # il.default_input_processor = MapCompose(lambda v: v.strip(), remove_tags, replace_escape_chars)
        il.add_value('ingestion_timestamp', Utils.getingestion_timestamp())
        il.add_value('sourceName', 'SD_SepticTank_Licenses')
        il.add_value('url', 'http://denr.sd.gov/des/sw/SepticInstallers.aspx')
        il.add_value('permit_lic_eff_date', date_certified)
        il.add_value('City', city)
        il.add_value('State', state)
        il.add_value('Zip', zip_code)
        company_name = self._getDBA(business_name)[0]
        if len(company_name) <2:
            company_name=person_name
        il.add_value('company_name',company_name)
        il.add_value('permit_lic_desc', 'Waste Transporter Licenses for '+company_name)
        il.add_value('dba_name', self._getDBA(business_name)[1])
        il.add_value('county', county)
        il.add_value('permit_lic_exp_date',expire_date)
        il.add_value('location_address_string',location_address_string)
        il.add_value('company_phone',phone)
        il.add_value('person_name',self._getDBA(person_name)[0])
        il.add_value('permit_type', 'waste_transporter_license')
        yield il.load_item()
