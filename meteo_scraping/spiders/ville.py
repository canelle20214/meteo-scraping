import scrapy
from scrapy import Request
import unidecode
from meteo_scraping.items import DataBase

class VilleSpider(scrapy.Spider):
    name = 'ville'
    allowed_domains = ['www.linternaute.com']
    start_urls = ['https://www.linternaute.com/ville/ile-de-france/region-11/villes']

    try:
      base = DataBase('region')
      base.create_table('ville', name=db.String, cp=db.String)
    except:
      pass

    def start_requests(self):
        for url in self.start_urls:
          yield Request(url=url, callback=self.parse_ville)
            
    def parse_ville(self, response):
        liste_villes = response.css('.list--bullet')[1].css('li')

        for ville in liste_villes:
            item = VilleItem() 
            
            #nom de la ville
            try: 
              item['name'] = unidecode.unidecode("-".join(ville.css('a::text').extract()[0].split(' ')[:-1]).lower())
            except:
              item['name'] = 'None'
            
            #code postal de la ville
            try: 
              item['cp'] = ville.css('a::text').extract()[0].split(' ')[-1].replace("(","").replace(")","")
            except:
              item['cp'] = 'None'

            yield item
            self.base.add_row('ville',name=item['name'],cp=item['cp'])