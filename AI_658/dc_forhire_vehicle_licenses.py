# # -*- coding: utf-8 -*-

# '''
# Created on 2018-Nov-29 05:22:46
# TICKET NUMBER -AI_658
# @author: raosaheb.g
# '''

# from scrapy.loader import ItemLoader
# from scrapy.loader.processors import MapCompose
# from w3lib.html import remove_tags, replace_escape_chars

# from Data_scuff.spiders.AI_658.items import DcForhireVehicleLicensesSpiderItem
# from Data_scuff.spiders.__common import CommonSpider,CustomSettings
# from Data_scuff.utils.utils import Utils
# from scrapy.shell import inspect_response
# import scrapy

# class DcForhireVehicleLicensesSpider(CommonSpider):
#     name = 'dc_forhire_vehicle_licenses'
#     allowed_domains = ['quickbase.com']
#      # start_urls = ['https://octo.quickbase.com/db/bmh5ebhur?a=q&qid=5'
#     start_urls = ['https://octo.quickbase.com/db/bmh5ebhur?a=q&qid=5','https://octo.quickbase.com/db/bmh5ebhur?a=q&qid=8','https://octo.quickbase.com/db/bmh5ebhur?a=q&qid=6','https://octo.quickbase.com/db/bmh5ebhur?a=q&qid=7&qskip=0&qrppg=300','https://octo.quickbase.com/db/bmh5ebhur?a=q&qid=9&qskip=0&qrppg=600']
    
#     custom_settings = {
#         'FILE_NAME':Utils.getRundateFileName('DcForhireVehicleLicensesSpider'),
#         'JIRA_ID':'AI_658',
#         'HTTPCACHE_ENABLED':False,

#         # 'JOBDIR' : CustomSettings.getJobDirectory('DcForhireVehicleLicensesSpider'),
#         'TOP_HEADER':{   'company_email': 'Email__c',
#                          'company_name': 'Company Name',
#                          'company_phone': 'Phone',
#                          'location_address_string': 'Address__c',
#                          'permit_lic_status': 'Status__c',
#                          'permit_subtype': 'Authority__c',
#                          'permit_type': '',
#                          'permit_lic_desc':'',
#                          'person drivers license expiration date': 'Person Drivers License Expiration Date',
#                          'person drivers license state': 'Person Drivers License State',
#                          'person drivers license status': 'Person Drivers License Status',
#                          'person face id': 'Person Face ID',
#                          'person_name': 'Person Name',
#                          'person_subtype': ''},
#         'FIELDS_TO_EXPORT':[  
#                          'permit_lic_desc' ,                     
#                          'company_name',
#                          'company_phone',
#                          'company_email',
#                          'location_address_string',
#                          'permit_subtype',
#                          'permit_lic_status',
#                          'person_name',
#                          'person_subtype',
#                          'person face id',
#                          'person drivers license state',
#                          'person drivers license status',
#                          'person drivers license expiration date',
#                          'permit_type',
#                          'sourceName',
#                          'url',
#                          'ingestion_timestamp'],
#         'NULL_HEADERS':['person face id', 'person drivers license state', 'person drivers license status', 'person drivers license expiration date']
#         }

#     items={}
#     items['phone']=items['email']=items['address']=items['city']=items['state']=items['authority']=items['status']=items['driver']=''
#     permit_lic_desc=''
#     def parse(self, response):
#         if response.url.startswith('https://octo.quickbase.com/db/bmh5ebhur?a=q&qid=5'):
#             row = response.xpath('//*[@id="VR_bmh5ebhur_5"]/tbody/tr/td[2]/text()') 
#             for i in range(2,len(row)+2):
#                 self.items['name'] = response.xpath('//*[@id="VR_bmh5ebhur_5"]/tbody/tr['+str(i)+']/td[2]/text()').extract_first()
                
#                 self.items['phone'] = response.xpath('//*[@id="VR_bmh5ebhur_5"]/tbody/tr['+str(i)+']/td[3]/text()').extract_first()
#                 self.items['email'] = response.xpath('//*[@id="VR_bmh5ebhur_5"]/tbody/tr['+str(i)+']/td[4]/a/text()').extract_first()
#                 self.items['address'] = response.xpath('//*[@id="VR_bmh5ebhur_5"]/tbody/tr['+str(i)+']/td[5]/text()').extract_first()
#                 self.items['city'] = response.xpath('//*[@id="VR_bmh5ebhur_5"]/tbody/tr['+str(i)+']/td[6]/text()').extract_first()
#                 self.items['state'] = response.xpath('//*[@id="VR_bmh5ebhur_5"]/tbody/tr['+str(i)+']/td[7]/text()').extract_first()
#                 self.items['authority'] = response.xpath('//*[@id="VR_bmh5ebhur_5"]/tbody/tr['+str(i)+']/td[8]/text()').extract_first()
#                 self.items['status'] = response.xpath('//*[@id="VR_bmh5ebhur_5"]/tbody/tr['+str(i)+']/td[9]/text()').extract_first()
#                 self.items['driver'] = response.xpath('//*[@id="VR_bmh5ebhur_5"]/tbody/tr['+str(i)+']/td[10]/a/@href').extract_first()
#                 self.items['drivers'] = response.xpath('//*[@id="VR_bmh5ebhur_5"]/tbody/tr['+str(i)+']/td[10]/a/text()').extract_first()
#                 url = 'https://octo.quickbase.com/db/'+self.items['driver']
#                 yield scrapy.Request(url=url,callback=self.getpage,dont_filter=True,meta=self.items)

#         elif response.url.startswith('https://octo.quickbase.com/db/bmh5ebhur?a=q&qid=8'):
#             row = response.xpath('//*[@id="VR_bmh5ebhur_8"]/tbody/tr/td[2]/text()')
#             for j in range(2,len(row)+2):
#                 self.items['name'] = response.xpath('//*[@id="VR_bmh5ebhur_8"]/tbody/tr['+str(j)+']/td[2]/text()').extract_first()
                
#                 self.items['phone'] = response.xpath('//*[@id="VR_bmh5ebhur_8"]/tbody/tr['+str(j)+']/td[3]/text()').extract_first()
#                 self.items['email'] = response.xpath('//*[@id="VR_bmh5ebhur_8"]/tbody/tr['+str(j)+']/td[4]/a/text()').extract_first()
#                 self.items['address'] = response.xpath('//*[@id="VR_bmh5ebhur_8"]/tbody/tr['+str(j)+']/td[5]/text()').extract_first()
#                 self.items['city'] = response.xpath('//*[@id="VR_bmh5ebhur_8"]/tbody/tr['+str(j)+']/td[6]/text()').extract_first()
#                 self.items['state'] = response.xpath('//*[@id="VR_bmh5ebhur_8"]/tbody/tr['+str(j)+']/td[7]/text()').extract_first()
#                 self.items['authority'] = response.xpath('//*[@id="VR_bmh5ebhur_8"]/tbody/tr['+str(j)+']/td[8]/text()').extract_first()
#                 self.items['status'] = response.xpath('//*[@id="VR_bmh5ebhur_8"]/tbody/tr['+str(j)+']/td[9]/text()').extract_first()
#                 self.items['driver'] = response.xpath('//*[@id="VR_bmh5ebhur_8"]/tbody/tr['+str(j)+']/td[10]/a/@href').extract_first()
#                 self.items['drivers'] = response.xpath('//*[@id="VR_bmh5ebhur_8"]/tbody/tr['+str(j)+']/td[10]/a/text()').extract_first()
#                 url = 'https://octo.quickbase.com/db/'+self.items['driver']
#                 yield scrapy.Request(url=url,callback=self.getpage,dont_filter=True,meta=self.items)

#         if response.url.startswith('https://octo.quickbase.com/db/bmh5ebhur?a=q&qid=6'):

#             row = response.xpath('//*[@id="VR_bmh5ebhur_6"]/tbody/tr/td[2]/text()')
#             for k in range(2,len(row)+2):
#                 self.items['name'] = response.xpath('//*[@id="VR_bmh5ebhur_6"]/tbody/tr['+str(k)+']/td[2]/text()').extract_first()
                
#                 self.items['phone'] = response.xpath('//*[@id="VR_bmh5ebhur_6"]/tbody/tr['+str(k)+']/td[3]/text()').extract_first()
#                 self.items['email'] = response.xpath('//*[@id="VR_bmh5ebhur_6"]/tbody/tr['+str(k)+']/td[4]/a/text()').extract_first()
#                 self.items['address'] = response.xpath('//*[@id="VR_bmh5ebhur_6"]/tbody/tr['+str(k)+']/td[5]/text()').extract_first()
#                 self.items['city'] = response.xpath('//*[@id="VR_bmh5ebhur_6"]/tbody/tr['+str(k)+']/td[6]/text()').extract_first()
#                 self.items['state'] = response.xpath('//*[@id="VR_bmh5ebhur_6"]/tbody/tr['+str(k)+']/td[7]/text()').extract_first()
#                 self.items['authority'] = response.xpath('//*[@id="VR_bmh5ebhur_6"]/tbody/tr['+str(k)+']/td[8]/text()').extract_first()
#                 self.items['status'] = response.xpath('//*[@id="VR_bmh5ebhur_6"]/tbody/tr['+str(k)+']/td[9]/text()').extract_first()
#                 self.items['driver'] = response.xpath('//*[@id="VR_bmh5ebhur_6"]/tbody/tr['+str(k)+']/td[10]/a/@href').extract_first()
#                 self.items['drivers'] = response.xpath('//*[@id="VR_bmh5ebhur_6"]/tbody/tr['+str(k)+']/td[10]/a/text()').extract_first()
#                 url = 'https://octo.quickbase.com/db/'+self.items['driver']
#                 yield scrapy.Request(url=url,callback=self.getpage,dont_filter=True,meta=self.items)


#         elif response.url.startswith('https://octo.quickbase.com/db/bmh5ebhur?a=q&qid=7&qskip=0&qrppg=300'):

#             row = response.xpath('//*[@id="VR_bmh5ebhur_7"]/tbody/tr/td[2]/text()')
#             for l in range(2,len(row)+2):
#                 self.items['name'] = response.xpath('//*[@id="VR_bmh5ebhur_7"]/tbody/tr['+str(l)+']/td[2]/text()').extract_first()
                
#                 self.items['phone'] = response.xpath('//*[@id="VR_bmh5ebhur_7"]/tbody/tr['+str(l)+']/td[3]/text()').extract_first()
#                 self.items['email'] = response.xpath('//*[@id="VR_bmh5ebhur_7"]/tbody/tr['+str(l)+']/td[4]/a/text()').extract_first()
#                 self.items['address'] = response.xpath('//*[@id="VR_bmh5ebhur_7"]/tbody/tr['+str(l)+']/td[5]/text()').extract_first()
#                 self.items['city'] = response.xpath('//*[@id="VR_bmh5ebhur_7"]/tbody/tr['+str(l)+']/td[6]/text()').extract_first()
#                 self.items['state'] = response.xpath('//*[@id="VR_bmh5ebhur_7"]/tbody/tr['+str(l)+']/td[7]/text()').extract_first()
#                 self.items['authority'] = response.xpath('//*[@id="VR_bmh5ebhur_7"]/tbody/tr['+str(l)+']/td[8]/text()').extract_first()
#                 self.items['status'] = response.xpath('//*[@id="VR_bmh5ebhur_7"]/tbody/tr['+str(l)+']/td[9]/text()').extract_first()
#                 self.items['driver'] = response.xpath('//*[@id="VR_bmh5ebhur_7"]/tbody/tr['+str(l)+']/td[10]/a/@href').extract_first()
#                 self.items['drivers'] = response.xpath('//*[@id="VR_bmh5ebhur_7"]/tbody/tr['+str(l)+']/td[10]/a/text()').extract_first()
#                 url = 'https://octo.quickbase.com/db/'+self.items['driver']
#                 yield scrapy.Request(url=url,callback=self.getpage,dont_filter=True,meta=self.items)


#         elif response.url.startswith('https://octo.quickbase.com/db/bmh5ebhur?a=q&qid=9&qskip=0&qrppg=600'):

#             row = response.xpath('//*[@id="VR_bmh5ebhur_9"]/tbody/tr/td[2]/text()')
#             for m in range(2,len(row)+2):
#                 self.items['name'] = response.xpath('//*[@id="VR_bmh5ebhur_9"]/tbody/tr['+str(m)+']/td[2]/text()').extract_first()
                
#                 self.items['phone'] = response.xpath('//*[@id="VR_bmh5ebhur_9"]/tbody/tr['+str(m)+']/td[3]/text()').extract_first()
#                 self.items['state'] = response.xpath('//*[@id="VR_bmh5ebhur_9"]/tbody/tr['+str(m)+']/td[7]/text()').extract_first()
#                 self.items['authority'] = response.xpath('//*[@id="VR_bmh5ebhur_9"]/tbody/tr['+str(m)+']/td[8]/text()').extract_first()
#                 self.items['status'] = response.xpath('//*[@id="VR_bmh5ebhur_9"]/tbody/tr['+str(m)+']/td[9]/text()').extract_first()
                
#                 url = 'https://octo.quickbase.com/db/'+self.items['driver']

#                 yield scrapy.Request(url=url,callback=self.getpage,dont_filter=True,meta=self.items)







#     def getpage(self,response):
#         print("######################",response.url)
#         meta_data=response.meta
#         if meta_data['authority']=='Taxicab Company':
#             self.permit_lic_desc='Taxicab Companies'
#         elif meta_data['authority']=='Taxicab Associations':
#             self.permit_lic_desc='Taxicab Association'
#         elif meta_data['authority'] == 'Limo Company':
#             self.permit_lic_desc='Limo Companies'
#         elif meta_data['authority']=='Independent Limo Operator':
#             self.permit_lic_desc='Independent Limousine Companies'
#         elif meta_data['authority'] == '':
#             self.permit_lic_desc='Independent Taxicab Companies'

#         if response.url=='https://octo.quickbase.com/db/bmh5ebhur?a=q&qid=9&qskip=0&qrppg=600':
            


#             _trs_list=response.xpath('//*[@id="facetResults"]/tbody/tr/td/div/div/table/tr[3]/td/table/tbody/tr')[2:]
#             print(_trs_list)

#             for tr in _trs_list:
#                 first_name=tr.xpath('./td[2]/text()').extract_first()
#                 print(first_name)
#                 last_name=tr.xpath('./td[3]/text()').extract_first()
#                 print(last_name)
#                 namee = str(first_name)+' '+str(last_name)
#                 face_id=tr.xpath('./td[4]/text()').extract_first()
#                 print(face_id)
#                 driver_license_state=tr.xpath('./td[5]/text()').extract_first()
#                 lic_status=tr.xpath('./td[6]/text()').extract_first()
#                 driver_license_exp_date=tr.xpath('./td[7]/text()').extract_first()
#                 print(driver_license_exp_date)
#                 company_name=tr.xpath('./td[9]/a/text()').extract_first()        
            
#                 il = ItemLoader(item=DcForhireVehicleLicensesSpiderItem(),response=response)
#                 il.default_input_processor = MapCompose(lambda v: v.strip(), remove_tags, replace_escape_chars)
#                 il.add_value('ingestion_timestamp', Utils.getingestion_timestamp())
#                 il.add_value('permit_lic_desc',self.permit_lic_desc)
#                 il.add_value('sourceName', 'DC_ForHire_Vehicle_Licenses')
#                 il.add_value('url', 'https://octo.quickbase.com/db/bkzdccmbr')
#                 il.add_value('company_name', meta_data['name'])
#                 il.add_value('company_phone', meta_data['phone'])
#                 il.add_value('company_email', meta_data['email'])
#                 il.add_value('location_address_string', meta_data['address']+', '+self.items['city']+' '+ self.items['state'])
#                 il.add_value('permit_subtype', meta_data ['authority'])
#                 il.add_value('permit_lic_status', meta_data['status'])
#                 il.add_value('person_name', namee)
#                 il.add_value('person_subtype', meta_data['drivers'])
#                 il.add_value('person face id',face_id)
#                 il.add_value('person drivers license state', driver_license_state)
#                 il.add_value('person drivers license status', lic_status)
#                 il.add_value('person drivers license expiration date', driver_license_exp_date)
#                 il.add_value('permit_type', 'transportation_license')
#                 yield il.load_item()
        
#         else:

#             _trs_list=response.xpath('//*[@id="facetResults"]/tbody/tr/td/div/div/table/tr[3]/td/table/tbody/tr')[2:]

#             for tr in _trs_list:
#                 first_name=tr.xpath('./td[2]/text()').extract_first()
#                 last_name=tr.xpath('./td[3]/text()').extract_first()
#                 namee = str(first_name)+' '+str(last_name)
#                 face_id=tr.xpath('./td[4]/text()').extract_first()
#                 print(face_id)
#                 driver_license_state=tr.xpath('./td[5]/text()').extract_first()
#                 lic_status=tr.xpath('./td[6]/text()').extract_first()
#                 driver_license_exp_date=tr.xpath('./td[7]/text()').extract_first()
#                 print(driver_license_exp_date)
#                 company_name=tr.xpath('./td[9]/a/text()').extract_first()  

#                 il = ItemLoader(item=DcForhireVehicleLicensesSpiderItem(),response=response)
#                 il.default_input_processor = MapCompose(lambda v: v.strip(), remove_tags, replace_escape_chars)
#                 il.add_value('ingestion_timestamp', Utils.getingestion_timestamp())
                
#                 il.add_value('permit_lic_desc',self.permit_lic_desc)
#                 il.add_value('sourceName', 'DC_ForHire_Vehicle_Licenses')
#                 il.add_value('url', 'https://octo.quickbase.com/db/bkzdccmbr')
#                 il.add_value('company_name', meta_data['name'])
#                 il.add_value('company_phone', meta_data['phone'])
#                 il.add_value('company_email', meta_data['email'])
#                 il.add_value('location_address_string', meta_data['address']+', '+meta_data['city']+' '+ meta_data['state'])
#                 il.add_value('permit_subtype', meta_data ['authority'])
#                 il.add_value('permit_lic_status', meta_data['status'])
#                 il.add_value('person_name', namee)
#                 il.add_value('person_subtype', meta_data['drivers'])
#                 il.add_value('person face id',face_id)
#                 il.add_value('person drivers license state', driver_license_state)
#                 il.add_value('person drivers license status', lic_status)
#                 il.add_value('person drivers license expiration date', driver_license_exp_date)
#                 il.add_value('permit_type', 'transportation_license')
#                 yield il.load_item()



