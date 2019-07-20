# -*- coding: utf-8 -*-

'''
Created on 2018-Nov-09 09:59:51
TICKET NUMBER -AI_567
@author: ait-python
'''


import tabula
import pandas as pd
import re
    
from scrapy.loader import ItemLoader
from scrapy.loader.processors import MapCompose
from w3lib.html import remove_tags, replace_escape_chars

from Data_scuff.spiders.AI_567.items import UtWasteTransportersSpiderItem
from Data_scuff.spiders.__common import CommonSpider,CustomSettings
from Data_scuff.utils.utils import Utils
from scrapy.loader import ItemLoader
from scrapy.loader.processors import MapCompose
from w3lib.html import remove_tags, replace_escape_chars
import scrapy
import re
import pdfquery
from pdfquery.cache import FileCache
import requests
import io
from dateutil.parser import parse

class UtWasteTransportersSpider(CommonSpider):
    name = 'ut_waste_transporters'
    allowed_domains = ['utah.gov']
    start_urls = ['https://deq.utah.gov/waste-management-and-radiation-control/hazardous-waste-management-program']
    link='https://deq.utah.gov/waste-management-and-radiation-control/hazardous-waste-management-program'
    
    custom_settings = {
        'FILE_NAME':Utils.getRundateFileName('Licenses_WasteTransporters_UT_CurationReady'),
        'JIRA_ID':'AI_567',
        # 'JOBDIR' : CustomSettings.getJobDirectory('UtWasteTransportersSpider'),
        'TOP_HEADER':{                        'company_name': 'Handler Name',
                         'county': 'County',
                         'generator status': 'Generator Status',
                         'location_address_string': 'Location Address+City+UT+Zipcode',
                         'operating tsdf universe': 'Operating TSDF Universe',
                         'permit_lic_no': 'Handler ID',
                         'permit_type': '',
                         'st. gen. status': 'St. Gen. Status',
                         'transfer facility': 'Transfer Facility',
                         'transporter ': 'Transporter '},
        'FIELDS_TO_EXPORT':['permit_lic_no','company_name','location_address_string','county','operating tsdf universe','generator status','st. gen. status','transporter ','transfer facility','permit_type','sourceName','url','ingestion_timestamp'],
        'NULL_HEADERS':['transporter ', 'generator status', 'st. gen. status',  'transfer facility']
        }


    def parse(self,response):
        table = response.xpath('//*[@id="post-12625"]/section/ul[3]/li[1]/a/@href').extract()

        yield scrapy.Request(url = self.link, callback=self.parse_pdf, dont_filter=True)

        


    def __extractData(self,response):
        # pd.set_option('display.max_rows', 500)
        # pd.set_option('display.max_columns', 500)
        # pd.set_option('display.width', 1000)
    
        def rolling_group(val):
            if pd.notnull(val) and 'UT' in val: 
                rolling_group.group += 1
            return rolling_group.group

        rolling_group.group = 0  


        def joinFunc(g, column):
            col = g[column]
            joiner = "/"
            s = joiner.join([str(each) for each in col if pd.notnull(each)])
            s = re.sub("(?<=&)" + joiner, " ", s)  
            s = re.sub("(?<=-)" + joiner, " ", s)  
            s = re.sub(joiner * 2, joiner, s)
            return s

        def getDf(temp_file, area):

            return tabula.read_pdf(temp_file,
                    pages='3-111',
                    encoding='ISO-8859-1',
                    area=area,
                    guess=False,
                    pandas_options={'header': 'infer'}
                    ).replace('\r', ' ', regex=True).dropna(how='all')

        df = getDf('/home/ait-python/Desktop/DSHW-2017-001263.pdf', [100,32.67,578.655,745.47])
        df.columns = ['Handler ID', 'Handler Name', 'Location Address', 'Location City', 'Zip Code', 'State District', 'Oper.TSDF', 'Gen status', 'St. Gen status', 'Transporter', 'Transfer Facility'] 
        groups = df.groupby(df['Handler ID'].apply(rolling_group), as_index=False)
        groupFunct = lambda g: pd.Series([joinFunc(g, col) for col in g.columns], index=g.columns)
        final_df = groups.apply(groupFunct).fillna('')
        # print("################################3",final_df[])

        yield final_df.to_dict('records')
        
    

       

    def parse_pdf(self,response):

        for col in self.__extractData(response):
            for row in col:
                list = []

                il = ItemLoader(item=UtWasteTransportersSpiderItem(),response=response)
                il.default_input_processor = MapCompose(lambda v: v.strip(), remove_tags, replace_escape_chars)
                il.add_value('ingestion_timestamp', Utils.getingestion_timestamp())
                il.add_value('sourceName', 'UT_Waste_Transporters')
                il.add_value('url', 'https://deq.utah.gov/waste-management-and-radiation-control/hazardous-waste-management-program')
                if 'CEG' in row['Gen status']:
                    row['Gen status'] ='Conditionally Execpt Small Quantity Generator'
                elif 'LQG' in row['Gen status']:
                    row['Gen status'] = 'Large Quantity Generator'
                elif 'SQG' in row['Gen status']:
                    row['Gen status'] = 'Small Quantity Generator'
                elif 'N' in row['Gen status']:
                    row['Gen status'] = 'Non-generator (verified)'
                il.add_value('generator status', row['Gen status'])

                if 'Y' in row['Transporter']:
                    row['Transporter'] = 'Yes'
                elif 'N' in row['Transporter']:
                    row['Transporter'] = 'No'

                il.add_value('transporter ', row['Transporter'])
                il.add_value('company_name', row['Handler Name'] )
                il.add_value('permit_type', 'waste_transporter_license')
                if '1' in row['St. Gen status']:
                    row['St. Gen status'] = 'LQG'
                elif '3' in row['St. Gen status']:
                    row['St. Gen status'] = 'CESQG'
                elif '4' in row['St. Gen status']:
                    row['St. Gen status'] ='One-time generator'
                elif '5' in row['St. Gen status']:
                    row['St. Gen status'] ='Periodic Generator'
                elif '6' in row['St. Gen status']:
                    row['St. Gen status'] ='Inactive'
                elif '7' in row['St. Gen status']:
                    row['St. Gen status'] = 'Closed'
                elif 'F' in row['St. Gen status']:
                    row['St. Gen status'] = 'Same as federally defined'

                il.add_value('st. gen. status', row['St. Gen status'] )
                if 'ST' in row['Oper.TSDF']:
                    row['Oper.TSDF']='Storage, Treatment'
                elif 'S' in row['Oper.TSDF'] :
                    row['Oper.TSDF']='Storage'
                elif 'T' in row['Oper.TSDF']:
                    row['Oper.TSDF']='Treatment'
                elif '-I-ST' in row['Oper.TSDF']:
                    row['Oper.TSDF']='Storage, Treatment, Incinerator'
                elif '------' in row['Oper.TSDF']:
                    row['Oper.TSDF']=' '
                il.add_value('operating tsdf universe', row['Oper.TSDF'])

                county=row['Zip Code']
                cou = county.split(" ")
                print("$$$$$$$$$$$$$$$$$$$$$",len(cou))
                print("$$$$$$$$$$$$$$$$$$$$$",row['State District'])
                print("@@@@@@@@@@@@@@@@@@@@@@@@@@",cou)
                if len(cou) == 3:
                    cou =cou[1] +' '+ cou[2]
                    il.add_value('county',cou )
                elif len(cou) == 2:
                    cou = cou[1]
                    il.add_value('county',cou )
                elif len(cou) == 1:
                    cou = ''
                    il.add_value('county',row['State District'])

                co=row['Zip Code']
                so = co.split(" ")
                

                    

                
                il.add_value('permit_lic_no',row['Handler ID'] )
                il.add_value('location_address_string',row['Location Address']+' '+row['Location City']+', UT '+ so[0].replace('.0',''))

                if 'Y' in row['Transfer Facility']:
                    row['Transfer Facility'] = 'Yes'
                elif 'N' in row['Transfer Facility']:
                    row['Transfer Facility'] = 'No'

                il.add_value('transfer facility', row['Transfer Facility'])
                

                yield il.load_item()



















































































































































        

    # def parse(self, response):

    #     df = tabula.read_pdf('/home/ait-python/Desktop/DSHW-2017-00462.pdf',
    
    #     pages = '3',delimeter=',',
    #     encoding='ISO-8859-1',
    #     area=(109.395,36.63,594.495,358.38),
    #     guess=False,
    #     pandas_options={'header': 'infer'})


        # df1 = tabula.read_pdf('/home/ait-python/Desktop/DSHW-2017-00462.pdf',
        # pages = '3',delimeter=',',
        # encoding='ISO-8859-1',
        # area=(109.395,354.42,594.495,498.96),
        # guess=False,
        # pandas_options={'header': 'infer'})
        

        
        # for (_, row), ( _, row1) in zip(df.iterrows(), df1.iterrows()):
        #     a=row.tolist()
        

        #     lic_no = a[0]
        #     if type(lic_no) is float:
        #         lic_no=''

           
            
        #     handler_name = a[1]
        #     if type(handler_name) is float:
        #         handler_name=''

        #     add_1 = a[2]
        #     if type(add_1) is float:
        #         add_1=''
            

        #     b=row1.tolist()
        #     add2=b[0]
        #     if type(add2) is float:
        #         add2=''

        #     add3=b[1]
        #     if type(add3) is float:
        #         add3=''

            

        #     address = add_1+', '+add2+', UT '+add3
        #     print("###########################",address)
            

            # il = ItemLoader(item=UtWasteTransportersSpiderItem(),response=response)
            # il.default_input_processor = MapCompose(lambda v: v.strip(), remove_tags, replace_escape_chars)
            # il.add_value('ingestion_timestamp', Utils.getingestion_timestamp())
            # # il.add_value('sourceName', 'ut_waste_transporters')
            # # il.add_value('url', 'https://deq.utah.gov/waste-management-and-radiation-control/hazardous-waste-management-program')
            # # il.add_value('generator status', '')
            # # il.add_value('transporter ', '')
            # il.add_value('company_name', handler_name)
            # # il.add_value('permit_type', '')
            # # il.add_value('st. gen. status', '')
            # # il.add_value('operating tsdf universe', '')
            # # il.add_value('county', '')
            # il.add_value('permit_lic_no', lic_no)
            # # il.add_value('transfer facility', '')
            # il.add_value('location_address_string',address)
            # yield il.load_item()




       
        

        # df2 = tabula.read_pdf('/home/ait-python/Desktop/DSHW-2017-00462.pdf',
        # pages = '3',delimeter=',',
        # encoding='ISO-8859-1',
        # area=(109.395,496.98,594.495,761.31),
        # guess=False,
        # pandas_options={'header': 'infer'})
        # for _, row in df2.iterrows():
        #     self.c=row.tolist()
        #     county = self.c[0]
        #     operating = self.c[1]
        #     if 'ST' in operating:
        #         operating='Storage'+','+' Treatment'
        #     elif 'S' in operating:
        #         operating = 'Storage'
        #     elif 'T' in operating :
        #         operating ='Treatment'
        #     elif '------' in operating:
        #         operating=''
        #     generator_status = self.c[2]
        #     if 'CEG' in generator_status:
        #         generator_status ='Conditionally Execpt Small Quantity Generator'
        #     elif 'SQG' in generator_status:
        #         generator_status = 'Small Quantity Generator'
        #     elif 'N' in generator_status:
        #         generator_status = 'Non-generator (verified)'
        #     elif 'LQG' in generator_status:
        #         generator_status = 'Large Quantity Generator'
        #     st_gen_status = self.c[3]
        #     if '1' in st_gen_status:
        #         st_gen_status = 'LQG'
        #     elif '3' in st_gen_status:
        #         st_gen_status = 'CESQG'
        #     elif '4' in st_gen_status:
        #         st_gen_status ='One-time generator'
        #     elif '5' in st_gen_status:
        #         st_gen_status ='Periodic Generator'
        #     elif '6' in st_gen_status:
        #         st_gen_status ='Inactive'
        #     elif '7' in st_gen_status:
        #         st_gen_status = 'Closed'
        #     elif 'F' in st_gen_status:
        #         st_gen_status = 'Same as federally defined'
        #     transporter = self.c[4]
        #     if 'Y' in transporter:
        #         transporter = 'Yes'
        #     elif 'N' in transporter:
        #         transporter = 'No'
        #     transfer = self.c[5]
        #     if 'Y' in transfer:
        #         transfer = 'Yes'
        #     elif 'N' in transfer:
        #         transfer = 'No'


  
        # # self.state['items_count'] = self.state.get('items_count', 0) + 1
        #     il = ItemLoader(item=UtWasteTransportersSpiderItem(),response=response)
        #     il.default_input_processor = MapCompose(lambda v: v.strip(), remove_tags, replace_escape_chars)
        #     il.add_value('ingestion_timestamp', Utils.getingestion_timestamp())
        #     il.add_value('sourceName', 'ut_waste_transporters')
        #     il.add_value('url', 'https://deq.utah.gov/waste-management-and-radiation-control/hazardous-waste-management-program')
        #     il.add_value('generator status', generator_status)
        #     il.add_value('transporter ', transporter)
        #     # il.add_value('company_name', handler_name)
        #     il.add_value('permit_type', 'waste_transporter_license')
        #     il.add_value('st. gen. status', st_gen_status)
        #     il.add_value('operating tsdf universe', operating)
        #     il.add_value('county', county)
        #     # il.add_value('permit_lic_no', lic_no)
        #     il.add_value('transfer facility', transfer)
        #     # il.add_value('location_address_string',)
        #     yield il.load_item()