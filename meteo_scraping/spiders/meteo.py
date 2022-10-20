import scrapy
from scrapy import Request
from meteo_scraping.spiders.ville import VilleSpider
from meteo_scraping.items import MeteoItem


class MeteoSpider(scrapy.Spider):
    name = 'meteo'
    allowed_domains = ['meteo-villes.com']
    
    try:
        town_list = VilleSpider().start_requests()
        start_urls = [f'https://www.meteo-villes.com/previsions-meteo-{town}-{postcode}' for (town, postcode) in town_list]
    except:
        start_urls = ['https://www.meteo-villes.com/previsions-meteo-paris-75000']

    def start_requests(self):
        for url in self.start_urls:
            yield Request(url=url, callback=self.parse_weather)
        
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