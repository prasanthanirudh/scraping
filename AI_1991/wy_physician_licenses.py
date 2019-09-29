# -*- coding: utf-8 -*-

'''
Created on 2019-Sep-25 05:37:42
TICKET NUMBER -AI_1991
@author: ait-python
'''

from scrapy.loader import ItemLoader
from scrapy.loader.processors import MapCompose
from w3lib.html import remove_tags, replace_escape_chars

from Data_scuff.spiders.AI_1991.items import WyPhysicianLicensesSpiderItem
from Data_scuff.spiders.__common import CommonSpider,CustomSettings
from Data_scuff.utils.utils import Utils
import scrapy
from scrapy.shell import inspect_response
import re
import numpy as np
import pandas as pd
import tabula
import scrapy
import tempfile
import re
import os
import sre_constants
from Data_scuff.utils.searchCriteria import SearchCriteria


class WyPhysicianLicensesSpider(CommonSpider):
    name = '1991_wy_physician_licenses'
    allowed_domains = ['glsuite.us']
    start_urls = ['https://wybomprod.glsuite.us/GLSuiteWeb/Clients/WYBOM/Public/Licenseesearch.aspx?SearchType=Physician']
    
    custom_settings = {
        'FILE_NAME':Utils.getRundateFileName('AI-1991_Licenses_Physician_WY_CurationReady'),
        'JIRA_ID':'AI_1991',
        'COOKIES_ENABLED': True,
        'COOKIES_DEBUG':True,
        'HTTPCACHE_ENABLED': False,
        'RANDOM_PROXY_DISABLED': False,
        # 'JOBDIR' : CustomSettings.getJobDirectory('wy_physician_licenses'),
        'TOP_HEADER':{                        'board certification': 'Board Certification',
                         'company_name': 'Licensee Name',
                         'company_phone': 'Phone',
                         'dba_name': '',
                         'disciplinary actions': 'Disciplinary Actions',
                         'location_address_string': 'Office Address',
                         'mail_address_string': '',
                         'permit_lic_desc': '',
                         'permit_lic_eff_date': 'Date Licensed',
                         'permit_lic_exp_date': 'Expiration Date',
                         'permit_lic_no': 'License Number',
                         'permit_lic_status': 'License Status',
                         'permit_subtype': 'Specialty',
                         'permit_type': '',
                         'reactivation date': 'Reactivation Date',
                         'sub-specialty': 'Sub-Specialty',
                         'violation_description': 'Disciplinary Summary',
                         'violation_type': ''},
        'FIELDS_TO_EXPORT':[                        
                         'company_name',
                         'dba_name',
                         'location_address_string',
                         'mail_address_string',
                         'company_phone',
                         'permit_lic_no',
                         'permit_lic_eff_date',
                         'reactivation date',
                         'permit_lic_exp_date',
                         'permit_lic_status',
                         'board certification',
                         'permit_subtype',
                         'sub-specialty',
                         'disciplinary actions',
                         'violation_description',
                         'violation_type',
                         'permit_lic_desc',
                         'permit_type',
                         'sourceName',
                         'url',
                         'ingestion_timestamp',

                         ],
        'NULL_HEADERS':['sub-specialty', 'board certification', 'disciplinary actions', 'reactivation date']
        }

    search_element=[] 
    option=[]

    check_first=True
    year=''
    number=[]
    yr=''
    years=[]
    def __init__(self, start=None, end=None,startnum=None,endnum=None, proxyserver=None, *a, **kw):
        super(WyPhysicianLicensesSpider, self).__init__(start,end, proxyserver=None,*a, **kw)
        self.year = SearchCriteria.strRange(self.start,self.end)

    def parse(self,response):
        if self.check_first:
            self.check_first = False
            # self.number = SearchCriteria.numberRange(self.startmm,self.endmm,1)
            self.number=response.xpath('//*[@id="bodyContent_ddlSpecialty"]/option/@value').extract()[1:]
            self.yr=self.year.pop(0)

        if len(self.number)>0:
            app=self.number.pop(0)   
            form_data={

                '__VIEWSTATE': response.xpath('//*[@id="__VIEWSTATE"]/@value').extract_first(),
                '__VIEWSTATEGENERATOR':response.xpath('//*[@id="__VIEWSTATEGENERATOR"]/@value').extract_first(),
                '__EVENTVALIDATION':response.xpath('//*[@id="__EVENTVALIDATION"]/@value').extract_first(),
                'ctl00$bodyContent$txtLicNum': '',
                'ctl00$bodyContent$txtLastName':str(self.yr),
                # 'ctl00$bodyContent$txtLastName':'Allen',
                'ctl00$bodyContent$txtFirstName':'',
                'ctl00$bodyContent$txtCity':'',
                'ctl00$bodyContent$ddlState': '',
                'ctl00$bodyContent$ddlSpecialty':str(app),
                # 'ctl00$bodyContent$ddlSpecialty':'',
                'ctl00$bodyContent$ddlBoardCert': '',
                'ctl00$bodyContent$btnSubmit': 'Perform Search',
            }
            print("________________________________",form_data['ctl00$bodyContent$ddlSpecialty'])
            print("________________________________",form_data['ctl00$bodyContent$txtLastName'])
            yield scrapy.FormRequest.from_response(response,formdata=form_data, callback = self.parse_two,method='POST',dont_filter=True)
    def parse_two(self,response):
        company_name=permit_lic_no=company_phone=location_address_string=permit_lic_exp_date=permit_lic_eff_date=permit_lic_status=reactivation_date=board_certificate=specilaity=sub_specialty=disciplinary=permit_subtype=''
        name=response.xpath("//table[@id='bodyContent_tblResults']").extract_first()
        name1=name.split('background-color:LightGrey;border-color:DarkGray;border-width:4px;border-style:Solid;') if name else ''
        for val in name1:
            value=val.split('</tr>')
            if 'href' in val:
                href=val.split('href=')[1].split(">")[0].replace('"','')
            for value1 in value:
                if value1 and 'Licensee Name' in value1:
                    company_name=self.data_clean(value1)
                    print('______________________________________-',company_name)

                elif value1 and 'License Number' in value1:
                    permit_lic_no=self.data_clean(value1)
                elif value1 and 'Office Address and Phone' in value1:
                    location=self.data_clean(value1)
                    match=re.search(r'\(?[\d]+\)?[\s][\d]+[-][\d]+',location)
                    if match:
                        company_phone = match.group()
                        location_address_string = location[:location.index(company_phone)]
                    else:
                        location_address_string = location
                        # location_address_string=location[:location.index(company_phone)]
                        company_phone=''
                elif value1 and 'Date Licensed' in value1:
                    permit_lic_eff_date=self.data_clean(value1)
                    print("----------------------------------------------------",permit_lic_eff_date)

                elif value1 and 'License Status' in value1:
                    permit_lic_exp=self.data_clean(value1)
                    if permit_lic_exp:
                        date_match=re.search(r'[\d]{1,3}[/][\d]{1,3}[/][\d]{1,4}',permit_lic_exp)
                        permit_lic_status=re.split(r';',permit_lic_exp)[1].replace('Board Certified','')
                        permit_lic_exp_date=date_match.group()
                    else:
                        permit_lic_exp_date=''
            
                elif value1 and 'Reactivation Date' in value1:
                    reactivation_date=self.data_clean(value1)
                elif value1 and 'Board Certification' in value1:
                    board_certificate=self.data_clean(value1).replace('Board Certified:','')
                elif value1 and 'Sub-Specialty'in value1:
                    sub_specialty=self.data_clean(value1)
                elif value1 and 'Specialty' in value1:
                    permit_subtype=self.data_clean(value1)
                elif value1 and 'Disciplinary Actions' in value1:
                    disciplinary=self.data_clean(value1)
                    il = ItemLoader(item=WyPhysicianLicensesSpiderItem(),response=response)
                    # il.default_input_processor = MapCompose(lambda v: v.strip(), remove_tags, replace_escape_chars)
                    il.add_value('ingestion_timestamp', Utils.getingestion_timestamp())
                    il.add_value('sourceName', 'WY_Physician_Licenses')
                    il.add_value('url', 'https://wybomprod.glsuite.us/GLSuiteWeb/Clients/WYBOM/Public/Licenseesearch.aspx?SearchType=Physician')
                    il.add_value('permit_type', 'medical_license')
                    il.add_value('board certification', board_certificate)
                    il.add_value('dba_name','')
                    il.add_value('permit_lic_status',permit_lic_status)
                    il.add_value('permit_lic_no',permit_lic_no)
                    il.add_value('permit_lic_eff_date',permit_lic_eff_date)
                    il.add_value('permit_subtype',permit_subtype)
                    il.add_value('violation_type', 'health_violation')
                    il.add_value('sub-specialty', sub_specialty)
                    il.add_value('reactivation date',reactivation_date)
                    il.add_value('location_address_string',location_address_string if location_address_string else 'WY')
                    il.add_value('permit_lic_desc', 'Physician License for '+str(company_name))
                    
                    if self.df(company_name):
                        il.add_value('violation_description',self.df(company_name).replace('Disciplinary Summary',''))
                        il.add_value('disciplinary actions', 'Yes')
                        il.add_value('mail_address_string',location_address_string)
                    else:
                        il.add_value('violation_description','') 
                        il.add_value('disciplinary actions', '')
                        il.add_value('mail_address_string', '')
                    il.add_value('company_phone',company_phone)
                    il.add_value('permit_lic_exp_date',permit_lic_exp_date)
                    il.add_value('company_name',company_name)
                    yield il.load_item()

        if len(self.number)>0:
            yield scrapy.Request(url=self.start_urls[0], callback=self.parse, dont_filter=True)
        elif len(self.year)>0:
            self.check_first=True
            yield scrapy.Request(url=self.start_urls[0], callback=self.parse, dont_filter=True)

    def df(self,vaal):
        import tabula
        import re
        df = tabula.read_pdf('https://da26d420-a-84cef9ff-s-sites.googlegroups.com/a/wyo.gov/wyomedboard/files-1/Discipline%20Report%209-18-2019.pdf?attachauth=ANoY7coROsLSnrNTPX6i5XMCJgj5my472u6SWrlUT0NPQzlvu15U_nr_XdVgDBv7Va7l4Xw1KQdt97dmsXhzbUeEEsYASJskIW477VWbQ_GNGI257-6PeH3X0DoUKBtQhtmsJY2dOlMqZI54S0KI0cZoi8_9L4jj_wS-lJTn2dGna7nA9o0PliCJ-DTgT5s0vWlB0f16qwzVIxHabDYEQWTA5wUsZQtmgz9rL9DXc43Eh4PsFaqdqwM%3D&attredirects=0',
        area = [2.678,59.67,783.743,589.815],
        columns=[175],
        pages= 'all',
        guess=False,
        encoding='ISO-8859-1',
        pandas_options={'header':None}).fillna('')
        df.columns=['a','b']
        start_index = df[df['a']=='Licensee Name'].index.tolist()
        index_list = [i for i in range(start_index[0])]
        df.drop(df.index[index_list], inplace=True)
        li2 = []
        for _, row in df.fillna('').iterrows():
            li2.extend(row)
        results = map(str,li2)
        v = re.compile(r'^Licensee Name.*$')
        match= list(filter(v.match,results))
        if match:
            df[1] = df.apply(lambda x: ' '.join(x.dropna().astype(str)), axis=1)
            def fillUniqueNums(v):
                if fillUniqueNums.change:
                    fillUniqueNums.unique_num += 1
                    fillUniqueNums.change = False
                if 'Licensee Name' in v[1]:
                    fillUniqueNums.change = True
                return str(fillUniqueNums.unique_num)
            fillUniqueNums.change = False
            fillUniqueNums.unique_num = 1
            df[0] = df.apply(lambda v:fillUniqueNums(v), axis=1)
            df[0]= df[0].shift(-1)
            df = df[[0, 1]]
            df = df.groupby(0)[1].apply(list)
            dict1={}
            dict2={}
            for data in df:
                overall=' '.join(data)
                na=re.split("Mailing Address",overall)[0]+re.search("Mailing Address",overall).group()
                name=na.replace('Mailing Address','').replace('Licensee Name','').strip().replace('.',',')
                date_match=re.search(r'[\d]{5}[\s]+[a-zA-Z].*',overall)
                if date_match:
                    ab=date_match.group()
                    zip_code=re.search(r'^[\d]{1,5}',ab)
                    desc=re.split(zip_code.group(),ab)[1]
                else:
                    date_match=re.search(r'[mM][aA][iI][lL][iI][nN][gG][\s]*[aA][dD][dD][rR][eE][sS][sS].*',overall)
                    if date_match:
                        ab=date_match.group().replace('Mailing Address','')
                        desc=ab
                dict1[name]=desc
                dict2.update(dict1)
        if vaal:
            description=dict2.get(vaal)
        else:
            description=''
        return description
                
    def data_clean(self, value):
        if value:
            try:
                clean_tags = re.compile('<.*?>')
                desc_list = re.sub('\s+', ' ', re.sub(clean_tags, '', value))
                desc_list_rep = desc_list.replace('Licensee Name', '').replace('License Number','').replace('Office Address and Phone','').replace('Date Licensed','').replace('Reactivation Date','').replace('License Status','').replace('Board Certification','').replace('Specialty','').replace('Sub-Specialty','').replace('Disciplinary Actions','').replace('Disciplinary Summary','').replace('Sub-','')
                return desc_list_rep.strip()
            except:
                return ''
        else:
            return ''
        