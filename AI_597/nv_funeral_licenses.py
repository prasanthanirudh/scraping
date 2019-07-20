# -*- coding: utf-8 -*-

'''
Created on 2018-Oct-29 10:20:54
TICKET NUMBER -AI_597
@author: ait-python
'''

from scrapy.loader import ItemLoader
from scrapy.loader.processors import MapCompose
from w3lib.html import remove_tags, replace_escape_chars

from Data_scuff.spiders.AI_597.items import NvFuneralLicensesSpiderItem
from Data_scuff.spiders.__common import CommonSpider,CustomSettings
from Data_scuff.utils.utils import Utils
import datetime
import pdfquery
import tabula
import pandas

class NvFuneralLicensesSpider(CommonSpider):
    name = 'nv_funeral_licenses'
    allowed_domains = ['nv.gov']
    start_urls = ['http://funeral.nv.gov/Licensees/Licensees/']
    
    custom_settings = {
        'FILE_NAME':Utils.getRundateFileName('Licenses_Funeral_NV_CurationReady'),
        'JIRA_ID':'AI_597',
        'JOBDIR' : CustomSettings.getJobDirectory('NvFuneralLicensesSpider'),
        'TOP_HEADER':{   'company_name': 'Facility DBA Name',
                         'company_phone': 'Phone',
                         'location_address_string': 'Physical Address+City++State+Zip',
                         'permit_lic_eff_date': 'Date Beginning',
                         'permit_lic_exp_date': 'Date Ending',
                         'permit_lic_no': 'License#/ Permit#',
                         'permit_subtype': 'License Type',
                         'permit_type': '',
                         'person_name': 'First Name+Middle Name+Last Name'},

        'FIELDS_TO_EXPORT':['permit_lic_no', 'person_name', 'company_name', 'permit_subtype', 'location_address_string', 'permit_lic_eff_date','permit_lic_exp_date','company_phone', 'permit_type', 'sourceName', 'url', 'ingestion_timestamp'],
        'NULL_HEADERS':[]
        }

    permit_lic_no=[]
    permit_lic_eff_date=[]
    permit_subtype=[]
    person_name=[]
    ingestion_timestamp=[]
    location_address_string=[]
    sourceName=[]
    company_phone=[]
    permit_type=[]
    permit_lic_exp_date=[]
    url=[]
    company_name=[]
    person_name1=[]
    person_name2=[]
    dob=[]
    doe=[]




    def parse(self, response):


        df = tabula.read_pdf('/home/ait-python/Desktop/583/1.pdf',
    
        pages = '1',delimeter=',',
        encoding='ISO-8859-1',
        area=(146.498,30.0,749.318,579.27),
        guess=False,
        pandas_options={'header': 'infer'})
        for _, row in df.iterrows():
            self.a=row.tolist()


            lic_no = str(self.a[0])

            print("###################",lic_no)
            fname = str(self.a[2])+" "+str(self.a[3])+" "+str(self.a[4])
            lname = fname.replace("nan","")
            lic_type = "Funeral Director License"
            daob = str(self.a[6])
            daoe = str(self.a[7])



            il = ItemLoader(item=NvFuneralLicensesSpiderItem(),response=response)
            il.default_input_processor = MapCompose(lambda v: v.strip(), remove_tags, replace_escape_chars)
            il.add_value('ingestion_timestamp', Utils.getingestion_timestamp())
            il.add_value('sourceName', 'NV_Funeral_Licenses')
            il.add_value('url', 'http://funeral.nv.gov/Licensees/Licensees/')
            il.add_value('permit_lic_no', lic_no)
            il.add_value('permit_lic_eff_date', daob)
            il.add_value('permit_subtype', lic_type)
            il.add_value('person_name', fname)
            il.add_value('location_address_string', 'NV')
            il.add_value('company_phone', '')
            il.add_value('permit_type', 'cemetery_funeral_license')
            il.add_value('permit_lic_exp_date', daoe)
            il.add_value('company_name', '')
            yield il.load_item()

        df2 = tabula.read_pdf('/home/ait-python/Downloads/pdf/Embalmers.pdf',
    
        pages = '2',delimeter=',',
        encoding='ISO-8859-1',
        area=(70.763,30.0,535.883,580.035),
        guess=False,
        pandas_options={'header': 'infer'})
        for _, row in df2.iterrows():
            self.b=row.tolist()
        
            lic_no = str(self.b[0])+str(self.b[1]).replace('nan','')
            fname = str(self.b[2])+" "+str(self.b[3])+" "+str(self.b[4]).replace('nan','')
            lname = fname.replace('nan','')
            lic_type = "Embalmer License"
            daob = str(self.b[6])
            daoe = str(self.b[7])


            il = ItemLoader(item=NvFuneralLicensesSpiderItem(),response=response)
            il.default_input_processor = MapCompose(lambda v: v.strip(), remove_tags, replace_escape_chars)
            il.add_value('ingestion_timestamp', Utils.getingestion_timestamp())
            il.add_value('sourceName', 'NV_Funeral_Licenses')
            il.add_value('url', 'http://funeral.nv.gov/Licensees/Licensees/')
            il.add_value('permit_lic_no', lic_no)
            il.add_value('permit_lic_eff_date', daob)
            il.add_value('permit_subtype', lic_type)
            il.add_value('person_name', lname)
            il.add_value('location_address_string', 'NV')
            il.add_value('company_phone', '')
            il.add_value('permit_type', 'cemetery_funeral_license')
            il.add_value('permit_lic_exp_date', daoe)
            il.add_value('company_name', '')
            yield il.load_item()


# #----------------------------------------------------------------------------------2
#         df3 = tabula.read_pdf('/home/ait-python/Downloads/pdf/FuneralArrangersLicensees.pdf',
#         pages = '1',delimeter=',',
#         encoding='ISO-8859-1',
#         area=(155.678,29.835,752.378,582.93),

#         guess=False,
#         pandas_options={'header': 'infer'})
#         for _, row in df3.iterrows():

#             self.c=row.tolist()

#             lic_no = str(self.c[0]).replace('nan','')
#             fname = str(self.c[1])+" "+str(self.c[2])+" "+str(self.c[3])
#             lname = fname.replace('nan','')
#             lic_type = "Funeral Arranger License"
#             daob = str(self.c[5]).replace('nan','')
#             daoe = str(self.c[6]).replace('nan','')

#             il = ItemLoader(item=NvFuneralLicensesSpiderItem(),response=response)
#             il.default_input_processor = MapCompose(lambda v: v.strip(), remove_tags, replace_escape_chars)
#             il.add_value('ingestion_timestamp', Utils.getingestion_timestamp())
#             il.add_value('sourceName', 'NV_Funeral_Licenses')
#             il.add_value('url', 'http://funeral.nv.gov/Licensees/Licensees/')
#             il.add_value('permit_lic_no', lic_no)
#             il.add_value('permit_lic_eff_date', daob)
#             il.add_value('permit_subtype', lic_type)
#             il.add_value('person_name', lname)
#             il.add_value('location_address_string', 'NV')
#             il.add_value('company_phone', '')
#             il.add_value('permit_type', 'cemetery_funeral_license')
#             il.add_value('permit_lic_exp_date', daoe)
#             il.add_value('company_name', '')
#             yield il.load_item()



#         df4 =tabula.read_pdf('/home/ait-python/Downloads/pdf/FuneralArrangersLicensees.pdf',
#         pages = '2',delimeter=',',
#         encoding='ISO-8859-1',
#         area=(60.818,30.0,767.678,583.86),
#         guess=False,
#         pandas_options={'header': 'infer'})
#         for _, row in df4.iterrows():

#             self.d=row.tolist()

#             lic_no = str(self.d[0]).replace('nan','')
#             fname = str(self.d[1])+" "+ str(self.d[2])+" "+str(self.d[3])
#             lname = fname.replace('nan','')
#             lic_type = "Funeral Arranger License"
#             daob = str(self.d[5])
#             daoe = str(self.d[6]).replace('nan','')
            
#             il = ItemLoader(item=NvFuneralLicensesSpiderItem(),response=response)
#             il.default_input_processor = MapCompose(lambda v: v.strip(), remove_tags, replace_escape_chars)
#             il.add_value('ingestion_timestamp', Utils.getingestion_timestamp())
#             il.add_value('sourceName', 'NV_Funeral_Licenses')
#             il.add_value('url', 'http://funeral.nv.gov/Licensees/Licensees/')
#             il.add_value('permit_lic_no', lic_no)
#             il.add_value('permit_lic_eff_date', daob)
#             il.add_value('permit_subtype', lic_type)
#             il.add_value('person_name', lname)
#             il.add_value('location_address_string', 'NV')
#             il.add_value('company_phone', '')
#             il.add_value('permit_type', 'cemetery_funeral_license')
#             il.add_value('permit_lic_exp_date', daoe)
#             il.add_value('company_name', '')
#             yield il.load_item()



#         df5 = tabula.read_pdf('/home/ait-python/Downloads/pdf/FuneralArrangersLicensees.pdf',
#         pages = '3',delimeter=',',
#         encoding='ISO-8859-1',
#         area=(60.818,30.0,393.593,580.035),
#         guess=False,
#         pandas_options={'header': 'infer'})
#         for _, row in df5.iterrows():
#             self.e = row.tolist()

#             lic_no = str(self.e[0]).replace('nan','')
#             fname = str(self.e[1])+" "+ str(self.e[2])+" "+str(self.e[3])
#             lname = fname.replace('nan','')
#             lic_type = "Funeral Arranger License"
#             daob = str(self.e[5])
#             daoe = str(self.e[6])
        
#             il = ItemLoader(item=NvFuneralLicensesSpiderItem(),response=response)
#             il.default_input_processor = MapCompose(lambda v: v.strip(), remove_tags, replace_escape_chars)
#             il.add_value('ingestion_timestamp', Utils.getingestion_timestamp())
#             il.add_value('sourceName', 'NV_Funeral_Licenses')
#             il.add_value('url', 'http://funeral.nv.gov/Licensees/Licensees/')
#             il.add_value('permit_lic_no', lic_no)
#             il.add_value('permit_lic_eff_date', daob)
#             il.add_value('permit_subtype', lic_type)
#             il.add_value('person_name', lname)
#             il.add_value('location_address_string', 'NV')
#             il.add_value('company_phone', '')
#             il.add_value('permit_type', 'cemetery_funeral_license')
#             il.add_value('permit_lic_exp_date', daoe)
#             il.add_value('company_name', '')
#             yield il.load_item()
    





# # #-----------------------------------------------------------------------------------------------------------------------------------3
#         df6 = tabula.read_pdf('/home/ait-python/Downloads/pdf/Funeral-Directors.pdf',
#         pages = '1',delimeter=',',
#         encoding='ISO-8859-1',
#         area=(146.498,30.0,763.853,580.035),
#         guess=False,
#         pandas_options={'header': 'infer'})
#         for _, row in df6.iterrows():
#             self.f = row.tolist()

#             lic_no = str(self.f[0]).replace('nan','')
#             fname = str(self.f[1])+" "+ str(self.f[2])+" "+str(self.f[3])
#             lname = fname.replace('nan','')
#             lic_type = "Funeral Director License"
#             daob = str(self.f[5])
#             daoe = str(self.f[6])
#             il = ItemLoader(item=NvFuneralLicensesSpiderItem(),response=response)
#             il.default_input_processor = MapCompose(lambda v: v.strip(), remove_tags, replace_escape_chars)
#             il.add_value('ingestion_timestamp', Utils.getingestion_timestamp())
#             il.add_value('sourceName', 'NV_Funeral_Licenses')
#             il.add_value('url', 'http://funeral.nv.gov/Licensees/Licensees/')
#             il.add_value('permit_lic_no', lic_no)
#             il.add_value('permit_lic_eff_date', daob)
#             il.add_value('permit_subtype', lic_type)
#             il.add_value('person_name', lname)
#             il.add_value('location_address_string', 'NV')
#             il.add_value('company_phone', '')
#             il.add_value('permit_type', 'cemetery_funeral_license')
#             il.add_value('permit_lic_exp_date', daoe)
#             il.add_value('company_name', '')
#             yield il.load_item()

        
#         df7 = tabula.read_pdf('/home/ait-python/Downloads/pdf/Funeral-Directors.pdf',
#         pages = '2,3,4',delimeter=',',
#         encoding='ISO-8859-1',
#         area=(60.818,30.0,751.613,580.035),
#         guess=False,
#         pandas_options={'header': 'infer'})
#         for _, row in df7.iterrows():
#             self.g = row.tolist()

#             lic_no = str(self.g[0]).replace('nan','')
#             fname = str(self.g[1])+" "+ str(self.g[2])+" "+str(self.g[3])
#             lname = fname.replace('nan','')
#             lic_type = "Funeral Director License"
#             daob = str(self.g[5])
#             daoe = str(self.g[6])
#             # print("^^^^^^^^^^^^^^^^^",daoe)
#             il = ItemLoader(item=NvFuneralLicensesSpiderItem(),response=response)
#             il.default_input_processor = MapCompose(lambda v: v.strip(), remove_tags, replace_escape_chars)
#             il.add_value('ingestion_timestamp', Utils.getingestion_timestamp())
#             il.add_value('sourceName', 'NV_Funeral_Licenses')
#             il.add_value('url', 'http://funeral.nv.gov/Licensees/Licensees/')
#             il.add_value('permit_lic_no', lic_no)
#             il.add_value('permit_lic_eff_date', daob)
#             il.add_value('permit_subtype', lic_type)
#             il.add_value('person_name', lname)
#             il.add_value('location_address_string', 'NV')
#             il.add_value('company_phone', '')
#             il.add_value('permit_type', 'cemetery_funeral_license')
#             il.add_value('permit_lic_exp_date', daoe)
#             il.add_value('company_name', '')
#             yield il.load_item()


# #-----------------------------------------------------


#         # df7 = tabula.read_pdf('/home/ait-python/Downloads/pdf/Funeral-Directors.pdf',
#         # pages = '3',
#         # encoding='ISO-8859-1',
#         # area=(60.818,30.0,751.613,580.035),
#         # guess=False,
#         # pandas_options={'header': 'infer'})
#         # for _, row in df7.iterrows():
#         #     self.g = row.tolist()

#         #     lic_no = str(self.g[0]).replace('nan','')
#         #     fname = str(self.g[1])+" "+ str(self.g[2])+" "+str(self.g[3])
#         #     lname = fname.replace('nan','')
#         #     lic_type = "Funeral Director License"
#         #     daob = str(self.g[5])
#         #     daoe = str(self.g[6])
#         #     # print("@@@@@@@@@@@@@@@@",daoe)

#         # df8 = tabula.read_pdf('/home/ait-python/Downloads/pdf/Funeral-Directors.pdf',
#         # pages = '4',
#         # encoding='ISO-8859-1',
#         # area=(60.818,30.0,751.613,580.035),
#         # guess=False,
#         # pandas_options={'header': 'infer'})
#         # for _, row in df8.iterrows():
#         #     self.h = row.tolist()

#         #     lic_no = str(self.h[0]).replace('nan','')
#         #     print("!!!!!!!!!!!!!!!!!!1",lic_no)
#         #     # fname = str(self.h[1])+" "+ str(self.h[2])+" "+str(self.h[3])
#         #     # lname = fname.replace('nan','')
#         #     # lic_type = "Funeral Director License"
#         #     # daob = str(self.h[5])
#         #     # daoe = str(self.h[6])
# #-----------------------------------------------------------------------------------------4









#         df9 = tabula.read_pdf('/home/ait-python/Downloads/pdf/FuneralEstablishmentsAndDirectCremationFacilities.pdf',
#         pages = '1',delimeter=',',
#         encoding='ISO-8859-1',
#         area=(163.845,23.76,574.695,768.24),
#         guess=False,
#         pandas_options={'header': 'infer'})
#         for _, row in df9.iterrows():
#             self.i = row.tolist()

#             lic_no = str(self.i[0]).replace('nan','')
#             cname = str(self.i[1])
#             ad = str(self.i[2])+", "+str(self.i[3])+", "+"NV "+str(self.i[4])
#             addr = ad.replace("nan","")
#             print("#@@#@#@#@#@#@#@#@#@#",addr)
#             phone = str(self.i[5]).replace("nan","")
#             lic_type = str(self.i[6]).replace("nan","")
#             il = ItemLoader(item=NvFuneralLicensesSpiderItem(),response=response)
#             il.default_input_processor = MapCompose(lambda v: v.strip(), remove_tags, replace_escape_chars)
#             il.add_value('ingestion_timestamp', Utils.getingestion_timestamp())
#             il.add_value('sourceName', 'NV_Funeral_Licenses')
#             il.add_value('url', 'http://funeral.nv.gov/Licensees/Licensees/')
#             il.add_value('permit_lic_no', lic_no)
#             il.add_value('permit_lic_eff_date','')
#             il.add_value('permit_subtype', lic_type)
#             il.add_value('person_name', '')
#             il.add_value('location_address_string', addr)
#             il.add_value('company_phone', phone)
#             il.add_value('permit_type', 'cemetery_funeral_license')
#             il.add_value('permit_lic_exp_date', '')
#             il.add_value('company_name', cname)
#             yield il.load_item()
            



#         df10 = tabula.read_pdf('/home/ait-python/Downloads/pdf/FuneralEstablishmentsAndDirectCremationFacilities.pdf',
#         pages = '2',delimeter=',',
#         encoding='ISO-8859-1',
#         area=(68.805,30.0,576.675,762.6),
#         guess=False,
#         pandas_options={'header': 'infer'})
#         for _, row in df10.iterrows():
#             self.j = row.tolist()

#             lic_no = str(self.j[0]).replace('nan','')
#             cname = str(self.j[1])
#             ad = str(self.j[2])+", "+str(self.j[3])+", "+"NV "+str(self.j[4])
#             addr = ad.replace("nan","")
#             phone = str(self.j[5]).replace("nan","")
#             lic_type = str(self.j[6]).replace("nan","")
#             print("#####################",lic_type)
#             il = ItemLoader(item=NvFuneralLicensesSpiderItem(),response=response)
#             il.default_input_processor = MapCompose(lambda v: v.strip(), remove_tags, replace_escape_chars)
#             il.add_value('ingestion_timestamp', Utils.getingestion_timestamp())
#             il.add_value('sourceName', 'NV_Funeral_Licenses')
#             il.add_value('url', 'http://funeral.nv.gov/Licensees/Licensees/')
#             il.add_value('permit_lic_no', lic_no)
#             il.add_value('permit_lic_eff_date','')
#             il.add_value('permit_subtype', lic_type)
#             il.add_value('person_name', '')
#             il.add_value('location_address_string', addr)
#             il.add_value('company_phone', phone)
#             il.add_value('permit_type', 'cemetery_funeral_license')
#             il.add_value('permit_lic_exp_date', '')
#             il.add_value('company_name', cname)
#             yield il.load_item()

#         df11 =tabula.read_pdf('/home/ait-python/Downloads/pdf/FuneralEstablishmentsAndDirectCremationFacilities.pdf',
#         pages = '3',delimeter=',',
#         encoding='ISO-8859-1',
#         area=(68.805,30.0,576.675,762.6),
#         guess=False,
#         pandas_options={'header': 'infer'})
#         for _, row in df11.iterrows():
#             self.k = row.tolist()

#             lic_no = str(self.k[0]).replace('nan','')
#             cname = str(self.k[1])
#             ad = str(self.k[2])+", "+str(self.k[3])+", "+"NV "+str(self.k[4])
#             addr = ad.replace("nan","")
#             phone = str(self.k[5]).replace("nan","")
#             lic_type = str(self.k[6]).replace("nan","")
#             print("#####################",lic_type)
#             il = ItemLoader(item=NvFuneralLicensesSpiderItem(),response=response)
#             il.default_input_processor = MapCompose(lambda v: v.strip(), remove_tags, replace_escape_chars)
#             il.add_value('ingestion_timestamp', Utils.getingestion_timestamp())
#             il.add_value('sourceName', 'NV_Funeral_Licenses')
#             il.add_value('url', 'http://funeral.nv.gov/Licensees/Licensees/')
#             il.add_value('permit_lic_no', lic_no)
#             il.add_value('permit_lic_eff_date','')
#             il.add_value('permit_subtype', lic_type)
#             il.add_value('person_name', '')
#             il.add_value('location_address_string', addr)
#             il.add_value('company_phone', phone)
#             il.add_value('permit_type', 'cemetery_funeral_license')
#             il.add_value('permit_lic_exp_date', '')
#             il.add_value('company_name', cname)
#             yield il.load_item()












        





















        # self.state['items_count'] = self.state.get('items_count', 0) + 1
        # il = ItemLoader(item=NvFuneralLicensesSpiderItem(),response=response)
        # il.default_input_processor = MapCompose(lambda v: v.strip(), remove_tags, replace_escape_chars)
        # #il.add_value('ingestion_timestamp', Utils.getingestion_timestamp())
        # il.add_value('sourceName', 'nv_funeral_licenses')
        # il.add_value('url', 'http://funeral.nv.gov/Licensees/Licensees/')
        # il.add_xpath('permit_lic_no', '')
        # il.add_xpath('permit_lic_eff_date', '')
        # il.add_xpath('permit_subtype', '')
        # il.add_xpath('person_name', '')
        # il.add_xpath('location_address_string', '')
        # il.add_xpath('company_phone', '')
        # il.add_xpath('permit_type', '')
        # il.add_xpath('permit_lic_exp_date', '')
        # il.add_xpath('company_name', '')
        # return il.load_item()