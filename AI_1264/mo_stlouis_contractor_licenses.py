# -*- coding: utf-8 -*-
'''
Created on 2019-May-17 04:08:01
TICKET NUMBER -AI_1264
@author: Prazi
'''
from scrapy.loader import ItemLoader
from scrapy.loader.processors import MapCompose
from w3lib.html import remove_tags, replace_escape_chars
from Data_scuff.spiders.AI_1264.items import MoStlouisContractorLicensesSpiderItem
from Data_scuff.spiders.__common import CommonSpider,CustomSettings
from Data_scuff.utils.utils import Utils
import tabula
import pandas as pd
import re
import numpy as np
import scrapy
import tempfile
import os
from inline_requests import inline_requests
class MoStlouisContractorLicensesSpider(CommonSpider):
	name = '1264_mo_stlouis_contractor_licenses'
	allowed_domains = ['stlouisco.com']
	start_urls = ['https://www.stlouisco.com/YourGovernment/PublicWorks/Licensing']
	custom_settings = {
		'FILE_NAME':Utils.getRundateFileName('AI-1264_Licenses_StLouis_Contractor_MO_CurationReady'),
		'JIRA_ID':'AI_1264',
		'HTTPCACHE_ENABLED':False,
		'COOKIES_ENABLED':True,
		'DOWNLOAD_DELAY':0.1,
		'COOKIES_DEBUG':True,
		'HTTPCACHE_ENABLED':False,
		# 'JOBDIR' : CustomSettings.getJobDirectory('mo_stlouis_contractor_licenses'),
		'TOP_HEADER':{'company_name': 'COMPANY NAME','company_phone': 'PHONE/Contact #','dba_name': '','location_address_string': 'BUSINESS LOCATION','mail_address_string': 'Mailing Address','permit_lic_desc': '','permit_subtype': 'Type','permit_type': '','person_name': 'LICENSE HOLDER','person_subtype': '','type/licensed contractors': 'TYPE/Licensed Contractors'},
		'FIELDS_TO_EXPORT':['permit_subtype','company_name','dba_name','person_name','person_subtype','type/licensed contractors','location_address_string','mail_address_string','company_phone','permit_lic_desc','permit_type','sourceName','url','ingestion_timestamp'],
		'NULL_HEADERS':['type/licensed contractors']
	}
	@inline_requests
	def parse(self, response):
		file1=response.xpath('//*[@id="dnn_ctr7787_HtmlModule_lblContent"]/div/p[1]/a/@href').extract_first()
		file2=response.xpath('//*[@id="dnn_ctr7787_HtmlModule_lblContent"]/div/p[3]/a/@href').extract_first()
		file3=response.xpath('//*[@id="dnn_ctr7787_HtmlModule_lblContent"]/div/p[5]/a/@href').extract_first()
		file4=response.xpath('//*[@id="dnn_ctr7787_HtmlModule_lblContent"]/div/p[7]/a/@href').extract_first()
		if file1:
			file2='elec'
			yield scrapy.Request(url='https://www.stlouisco.com/Portals/8/docs/document%20library/public%20works/code%20enforcement/licenses/elect/Elec-Contr.pdf',callback=self.pdf_content,dont_filter=True,meta={'file2':file2})
		if file2:
			file2='mech'
			yield scrapy.Request(url='https://www.stlouisco.com/Portals/8/docs/Document%20Library/Public%20Works/code%20enforcement/licenses/mech/Internet%20Contractor%20List%20-%20Jan%202019.pdf',callback=self.pdf_content,dont_filter=True,meta={'file2':file2})
		if file3:
			file2='plum'
			parse_value=yield scrapy.Request(url='https://www.stlouisco.com/Your-Government/Public-Works/Licensing/LicPlumb',dont_filter=True)
			link=parse_value.xpath('//*[@id="dnn_ctr8308_HtmlModule_lblContent"]/table//tr[2]/td[2]/ul/li/a/@href').extract()
			for i in link:
				link_url='https://www.stlouisco.com'+str(i)
				yield scrapy.Request(url=link_url,callback=self.pdf_content,dont_filter=True,meta={'file2':file2})
		if file4:
			file2='back'
			parse_value=yield scrapy.Request(url='https://www.stlouisco.com/Your-Government/Public-Works/Licensing/LicBackFlow',dont_filter=True)
			link=parse_value.xpath('//*[@id="dnn_ctr7825_HtmlModule_lblContent"]/table//tr[2]/td[2]/ul/li/a/@href').extract()
			for i in link:
				link_url='https://www.stlouisco.com'+str(i)
				yield scrapy.Request(url=link_url,callback=self.pdf_content,dont_filter=True,meta={'file2':file2})
	def pdf_content(self, response):
		meta={}
		file_name=response.meta['file2']
		url=response.url
		file=self.storeGet_tempfile(response)
		#electrical
		if str(file_name)=='elec':
			meta['permit_subtype']=meta['company_name']=meta['dba_name']=meta['person_name']=meta['person_subtype']=meta['type/licensed contractors']=meta['location_address_string']=meta['mail_address_string']=meta['company_phone']=meta['permit_lic_desc']=meta['permit_type']=''
			df =tabula.read_pdf(file,pages='all',encoding='ISO-8859-1',guess=False,columns=[211.905,333.54,391.68,520.2,593.64],area=[16.448,11.475,735.548,595.17],pandas_options={'header': 'infer'})
			for _, row in df.fillna('').iterrows():
				row=row.tolist()
				meta['permit_subtype']='ELECTRICAL LICENSING'
				company_name = row[0]
				meta['company_name'] =self._getDBA(company_name)[0]
				person_name=row[1]
				meta['person_name']=self._getDBA(person_name)[0]
				meta['person_subtype']='License Holder'
				meta['type/licensed contractors']=row[2]
				meta['location_address_string']=row[3]
				meta['company_phone']=row[4]
				meta['permit_lic_desc']='ELECTRICAL LICENSING'
				meta['permit_type']='electrical_contractor_license'
				dba_name1=self._getDBA(company_name)[1]
				dba_name2=self._getDBA(person_name)[1]
				if meta['company_name']:
					meta['company_name']=meta['company_name']
				else:
					if meta['person_name']:
						meta['company_name']=meta['person_name']
				if dba_name1:
					meta['dba_name']=dba_name1
					yield self.save_to_csv(response,**meta)
				if dba_name2:
					meta['dba_name']=dba_name2
					yield self.save_to_csv(response,**meta)
				else:
					yield self.save_to_csv(response,**meta)
		#mechanical
		if str(file_name)=='mech':
			meta['permit_subtype']=meta['company_name']=meta['dba_name']=meta['person_name']=meta['person_subtype']=meta['type/licensed contractors']=meta['location_address_string']=meta['mail_address_string']=meta['company_phone']=meta['permit_lic_desc']=meta['permit_type']=''
			df =tabula.read_pdf(file,pages='all',encoding='ISO-8859-1',guess=False,spreadsheet=True,pandas_options={'header': 'infer'})
			for _, row in df.fillna('').iterrows():
				row=row.tolist()
				meta['permit_subtype']='MECHANICAL LICENSING'
				meta['type/licensed contractors']='Licensed Mechanical Contractors'
				company_name = row[0]
				meta['company_name'] =self._getDBA(company_name)[0]
				meta['dba_name']=self._getDBA(company_name)[1]
				meta['location_address_string']=row[1]+', '+row[2]+', '+row[3]+' '+row[4]
				meta['company_phone']=row[5]
				meta['permit_lic_desc']='MECHANICAL LICENSING'
				meta['permit_type']='contractor_license'
				yield self.save_to_csv(response,**meta)
		#plumbing
		if str(file_name)=='plum':
			meta['permit_subtype']=meta['company_name']=meta['dba_name']=meta['person_name']=meta['person_subtype']=meta['type/licensed contractors']=meta['location_address_string']=meta['mail_address_string']=meta['company_phone']=meta['permit_lic_desc']=meta['permit_type']=''
			df =tabula.read_pdf(file,pages='all',encoding='ISO-8859-1',guess=True,stream=True,pandas_options={'header': 'infer'})
			for _, row in df.fillna('').iterrows():
				row=row.tolist()
				company_name = row[0]
				person_name= row[1]+' '+row[2]
				meta['company_name'] =self._getDBA(company_name)[0]
				meta['person_name']=self._getDBA(person_name)[0]
				meta['mail_address_string']=row[3]+', '+row[4]+', '+row[5]+' '+row[6]
				meta['company_phone']=row[7]
				meta['permit_type']='plumbing_contractor_license'
				meta['permit_subtype']='PLUMBING LICENSING'
				if meta['company_name']:
					meta['company_name']=meta['company_name']
				else:
					if meta['person_name']:
						meta['company_name']=meta['person_name']
				dba_name1=self._getDBA(company_name)[1]
				dba_name2=self._getDBA(person_name)[1]
				if str(url)=='https://www.stlouisco.com/Portals/8/docs/document%20library/public%20works/code%20enforcement/licenses/plumb/Master-Drainlayers.pdf?VR=0619':
					meta['permit_lic_desc']='PLUMBING LICENSING-Master Drainlayers'
					meta['type/licensed contractors']='Master Drainlayers'
					if dba_name1:
						meta['dba_name']=dba_name1
						yield self.save_to_csv(response,**meta)
					if dba_name2:
						meta['dba_name']=dba_name2
						yield self.save_to_csv(response,**meta)
					else:
						yield self.save_to_csv(response,**meta)
				elif str(url)=='https://www.stlouisco.com/Portals/8/docs/Document%20Library/Public%20Works/code%20enforcement/licenses/plumb/Monthly%20Public%20Information%20-%20Master%20PipeFitters_New.pdf':
					meta['permit_lic_desc']='PLUMBING LICENSING-Master PipeFitters'
					meta['type/licensed contractors']='Master PipeFitters'
					if dba_name1:
						meta['dba_name']=dba_name1
						yield self.save_to_csv(response,**meta)
					if dba_name2:
						meta['dba_name']=dba_name2
						yield self.save_to_csv(response,**meta)
					else:
						yield self.save_to_csv(response,**meta)
				elif str(url)=='https://www.stlouisco.com/Portals/8/docs/Document%20Library/Public%20Works/code%20enforcement/licenses/plumb/Master-Plumbers.pdf?VR=0619':
					meta['permit_lic_desc']='PLUMBING LICENSING-Master Plumbers'
					meta['type/licensed contractors']='Master Plumbers'
					if dba_name1:
						meta['dba_name']=dba_name1
						yield self.save_to_csv(response,**meta)
					if dba_name2:
						meta['dba_name']=dba_name2
						yield self.save_to_csv(response,**meta)
					else:
						yield self.save_to_csv(response,**meta)
				elif str(url)=='https://www.stlouisco.com/Portals/8/docs/Document%20Library/Public%20Works/code%20enforcement/licenses/plumb/Monthly%20Public%20Information%20-%20Master%20SprinklerFitters_New.pdf':
					meta['permit_lic_desc']='PLUMBING LICENSING-Master SprinklerFitters'
					meta['type/licensed contractors']='Master SprinklerFitters'
					if dba_name1:
						meta['dba_name']=dba_name1
						yield self.save_to_csv(response,**meta)
					if dba_name2:
						meta['dba_name']=dba_name2
						yield self.save_to_csv(response,**meta)
					else:
						yield self.save_to_csv(response,**meta)
				elif str(url)=='https://www.stlouisco.com/Portals/8/docs/document%20library/public%20works/code%20enforcement/licenses/plumb/Monthly%20Public%20Information%20-%20Master%20Water%20Heater%20Contractors.pdf':
					meta['permit_lic_desc']='PLUMBING LICENSING-Master Water Heater Contractors'
					meta['type/licensed contractors']='Master Water Heater Contractors'
					if dba_name1:
						meta['dba_name']=dba_name1
						yield self.save_to_csv(response,**meta)
					if dba_name2:
						meta['dba_name']=dba_name2
						yield self.save_to_csv(response,**meta)
					else:
						yield self.save_to_csv(response,**meta)
		#black Flow
		if str(file_name)=='back':
			meta['permit_subtype']=meta['company_name']=meta['dba_name']=meta['person_name']=meta['person_subtype']=meta['type/licensed contractors']=meta['location_address_string']=meta['mail_address_string']=meta['company_phone']=meta['permit_lic_desc']=meta['permit_type']=''
			if str(url)=='https://www.stlouisco.com/Portals/8/docs/Document%20Library/Public%20Works/code%20enforcement/licenses/Backflow/Lawn%20Irrigation%20Contractors.pdf' or str(url)=='https://www.stlouisco.com/Portals/8/docs/Document%20Library/Public%20Works/code%20enforcement/licenses/Backflow/Plumbing%20Contractors.pdf':
				def __extractPdf(self,response):
					df =tabula.read_pdf(file,pages='all',encoding='ISO-8859-1',guess=False,area=[88.358,22.95,738.608,597.465],columns=[154.53,285.345,415.925,543.15],pandas_options={'header': None})
					asd=[df[i] for i in df.columns.values]
					result=pd.concat(asd).reset_index(drop=True)
					df = result.to_frame(name=None)
					df[1] = df.apply(lambda x:x[0] if str(x[0]).startswith('(') else np.nan, axis = 1)
					def fillUniqueNum(v):
						if fillUniqueNum.change:
							fillUniqueNum.unique_num += 1
						fillUniqueNum.change = False
						if str(v[0]).startswith('('):
							fillUniqueNum.change = True
						return str(fillUniqueNum.unique_num)
					fillUniqueNum.change = False
					fillUniqueNum.unique_num = 1
					df[2]= df.apply(lambda v:fillUniqueNum(v), axis=1)
					df = df[[0, 1, 2]]
					df = df.groupby(2)
					for val, i in enumerate(df):
						x= pd.DataFrame(i[1]).reset_index(drop=True)
						x = x.drop(columns=2)
						if x.apply(len).values[0]>2:
							x = x.dropna(how='all')
							try:
								x[0] = x.apply(lambda x:x[0] if not str(x[0]).startswith('(') else np.nan, axis = 1)
								x = x.apply(lambda x: pd.Series(x.dropna().values))
								x[2] = x[0][1:]
								x = x.apply(lambda x: pd.Series(x.dropna().values))
								x[3] = ', '.join(x[2].tolist()[:-1])
								x = x.drop(columns=2)
								x= x.dropna()
								x.columns = ['company_name', 'phone', 'loc1']
								final_df = x.to_dict('records')
								yield final_df
							except ValueError:
								pass 
				for col in __extractPdf(file,response):
					for row in col:
						company_name=row['company_name']
						company_phone=row['phone']
						location_address_string=row['loc1']
						meta['permit_subtype']='BACKFLOW TESTING'
						meta['company_name']= self._getDBA(company_name)[0]
						meta['dba_name']=self._getDBA(company_name)[1]
						meta['permit_type']='contractor_license'
						meta['company_phone']=company_phone
						meta['location_address_string']=location_address_string
						if str(url)=='https://www.stlouisco.com/Portals/8/docs/Document%20Library/Public%20Works/code%20enforcement/licenses/Backflow/Lawn%20Irrigation%20Contractors.pdf':
							meta['permit_lic_desc']='BACKFLOW TESTING-Lawn Irrigation Contractors '
							meta['type/licensed contractors']='Lawn Irrigation Contractors'
							yield self.save_to_csv(response,**meta)
						elif str(url)=='https://www.stlouisco.com/Portals/8/docs/Document%20Library/Public%20Works/code%20enforcement/licenses/Backflow/Plumbing%20Contractors.pdf':
							meta['permit_lic_desc']='BACKFLOW TESTING-Plumbing Contractors'
							meta['type/licensed contractors']='Plumbing Contractors'
							yield self.save_to_csv(response,**meta)
			elif str(url)=='https://www.stlouisco.com/Portals/8/docs/document%20library/public%20works/code%20enforcement/licenses/backflow/Fire%20Suppression%20Contractors.pdf':
				df =tabula.read_pdf(file,pages='all',encoding='ISO-8859-1',guess=False,area=[88.358,22.95,738.608,597.465],columns=[154.53,285.345,416.925,543.15],pandas_options={'header': None})
				asd=[df[i] for i in df.columns.values]
				result=pd.concat(asd).reset_index(drop=True)
				df = result.to_frame(name=None)
				df[1] = df.apply(lambda x:x[0] if str(x[0]).startswith('(') else np.nan, axis = 1)
				df[1] = df[1].shift(-1)
				df[0] = df.apply(lambda x:x[0] if not str(x[0]).startswith('(') else np.nan, axis = 1)
				df = df.dropna(how='all')
				for _, row in df.fillna('').iterrows():
					row=row.tolist()
					meta['permit_subtype']='BACKFLOW TESTING'
					company_name=row[0]
					meta['company_name']= self._getDBA(company_name)[0]
					meta['dba_name']=self._getDBA(company_name)[1]
					meta['company_phone']=row[1]
					meta['location_address_string']='MO'
					meta['permit_lic_desc']='BACKFLOW TESTING-Fire Suppression Contractors'
					meta['permit_type']='contractor_license'
					meta['type/licensed contractors']='Fire Suppression Contractors'
					yield self.save_to_csv(response,**meta)
			elif str(url)=='https://www.stlouisco.com/Portals/8/docs/Document%20Library/Public%20Works/code%20enforcement/licenses/Backflow/Process%20Piping%20Contractors.pdf':
				df =tabula.read_pdf(file,pages='all',encoding='ISO-8859-1',guess=False,area=[88.358,22.95,738.608,597.465],columns=[154.53,285.345,416.925,543.15],pandas_options={'header': None})
				asd=[df[i] for i in df.columns.values]
				result=pd.concat(asd).reset_index(drop=True)
				df = result.to_frame(name=None)
				df[1] = df.apply(lambda x:x[0] if str(x[0]).startswith('(') else np.nan, axis = 1)
				df[1] = df[1].shift(-1)
				df[0] = df.apply(lambda x:x[0] if not str(x[0]).startswith('(') else np.nan, axis = 1)
				df = df.dropna(how='all')
				for _, row in df.fillna('').iterrows():
					row=row.tolist()
					meta['permit_subtype']='BACKFLOW TESTING'
					company_name=row[0]
					meta['company_name']= self._getDBA(company_name)[0]
					meta['dba_name']=self._getDBA(company_name)[1]
					meta['company_phone']=row[1]
					meta['location_address_string']='MO'
					meta['permit_lic_desc']='BACKFLOW TESTING-Process Piping Contractors'
					meta['permit_type']='contractor_license'
					meta['type/licensed contractors']='Process Piping Contractors'
					yield self.save_to_csv(response,**meta)

	def save_to_csv(self,response,**meta):
		il = ItemLoader(item=MoStlouisContractorLicensesSpiderItem())
		il.add_value('sourceName', 'MO_StLouis_Contractor_Licenses')
		il.add_value('url', 'https://www.stlouisco.com/YourGovernment/PublicWorks/Licensing')
		il.add_value('permit_subtype',meta['permit_subtype'])
		il.add_value('mail_address_string',meta['mail_address_string'])
		il.add_value('dba_name',meta['dba_name'])
		il.add_value('company_name',meta['company_name'])
		il.add_value('type/licensed contractors',meta['type/licensed contractors'])
		il.add_value('person_subtype',meta['person_subtype'])
		il.add_value('person_name',meta['person_name'])
		il.add_value('permit_type',meta['permit_type'])
		il.add_value('permit_lic_desc',meta['permit_lic_desc'])
		il.add_value('company_phone',meta['company_phone'])
		il.add_value('location_address_string',meta['location_address_string'])
		return il.load_item()
	def storeGet_tempfile(self,response):
		outfd, temp_path = tempfile.mkstemp(prefix='', suffix='')
		with os.fdopen(outfd, 'wb') as pdf_file:
			pdf_file.write(response.body)
		return temp_path
	
	