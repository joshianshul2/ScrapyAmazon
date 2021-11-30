import scrapy
from ..items import AmazonscrpyItem
import pandas as pd
from csv import writer
from pathlib import Path
import numpy as np
import pickle
import os
import time

my_scrap_count_path = Path("scrap_count.pickle")
my_scrap_count = 0

if my_scrap_count_path.exists():
    with open('scrap_count.pickle', 'rb') as handle:
        my_scrap_count = pickle.load(handle)['count']
else:
    my_scrap_count = 1
    with open('scrap_count.pickle', 'wb') as handle:
        pickle.dump({"count": 2}, handle)

output_df_path = Path("Scrapped_data.csv")
output_df = None

if output_df_path.exists():
    output_df = pd.read_csv("Scrapped_data.csv")

timestamp = time.time()
datetime = time.strftime('%A, %Y-%m-%d %H:%M:%S', time.localtime(timestamp))


class AmazonscrpySpider(scrapy.Spider):
    name = 'amazon'

    def __init__(self):
        self.priceList = []
        self.urlList = []

        if (output_df_path.exists()):
            self.priceList = [0] * len(output_df)

    def start_requests(self):
        url_df = pd.read_csv('Scrapping_Data.csv')
        self.urlList = url_df['URLS'].values
        for URL in self.urlList:
            yield scrapy.Request(url=URL, callback=self.parse)

    def parse(self, response):
        global output_df
        global my_scrap_count
        items = AmazonscrpyItem()

        product_name = response.css('#productTitle::text').extract()
        product_price = response.css('#priceblock_ourprice').css('::text').getall()
        product_details = response.css('#productDetails_detailBullets_sections1 tr:nth-child(1) .prodDetAttrValue').css('::text').getall()

        items['product_name'] = product_name
        items['product_price'] = product_price if product_price else ["Not Available"]
        items['product_details'] = product_details
        item_url = str(response).split()[1][:-1]

        yield items

        if my_scrap_count > 1:

            self.priceList[np.where(output_df.URLS == item_url)[0][0]] = items['product_price'][0]

            if len(self.priceList) == len(output_df):

                output_df[str(datetime)] = self.priceList
                output_df.to_csv("Scrapped_data.csv", index=False)

        else:

            if output_df_path.exists():

                dict = {'URLS': item_url,str(datetime): items['product_price'][0]}
                output_df = output_df.append(dict, ignore_index=True)

            else:
                dict = {'URLS': item_url, str(datetime): items['product_price']}
                output_df = pd.DataFrame(dict)

            output_df.to_csv("Scrapped_data.csv", index=False)







