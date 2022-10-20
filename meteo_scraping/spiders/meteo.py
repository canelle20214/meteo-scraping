import scrapy
from scrapy import Request
from meteo_scraping.items import MeteoItem
import unidecode
from meteo_scraping.items import DataBase


class MeteoSpider(scrapy.Spider):
    name = 'meteo'
    allowed_domains = ['www.linternaute.com', 'meteo-villes.com']
    
    start_urls = ['https://www.linternaute.com/ville/ile-de-france/region-11/villes']

    try:
      base = DataBase('region')
      base.create_table('ville', name=db.String, cp=db.String)
    except:
      pass

    def start_requests(self):
        Request(url=self.start_urls[0], callback=self.parse_ville)
        base = DataBase('region')
        town_list = base.read_table('ville')
        start_urls = [f'https://www.meteo-villes.com/previsions-meteo-{town}-{postcode}' for (town, postcode) in town_list]
        for url in start_urls:
            yield Request(url=url, callback=self.parse_weather, cb_kwargs=town_list)

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

            self.base.add_row('ville',name=item['name'],cp=item['cp'])
            yield item


    def parse_weather(self, response):
        today_weather = response.css("div.city-summary-line")[0]
        item = MeteoItem()
    
        #temperature min
        try: 
            item['low'] = meteo.css(".city-summary-line__temp div::text")[0].extract()
        except:
            item['low'] = 'None'
        
        #temperature max
        try: 
            item['hight'] = meteo.css(".city-summary-line__temp div::text")[1].extract()
        except:
            item['hight'] = 'None'
        
        #vent
        try: 
            item['wind'] = meteo.css(".city-summary-line__wind--speed span::text").extract()[0]
        except:
            item['wind'] = 'None'
        
        #pluie
        try: 
            item['rain'] = meteo.css(".city-summary-line__rain::text").extract()[0].strip()
        except:
            item['rain'] = 'None'
        
        #date
        try: 
            item['date'] = time.ctime()
        except:
            item['date'] = 'None'

        yield item