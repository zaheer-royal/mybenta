import scrapy, os, json
from scrapy.crawler import CrawlerProcess
from multiprocessing import Process
cwd = os.getcwd()
class CellNoSpider(scrapy.Spider):
    name = "cell_no"
    allowed_domains = ["mybenta.com"]
    # start_urls =[]
    file_path = os.path.join('output', 'url_from_robot_xml')
    if not os.path.exists(file_path):
        os.makedirs(file_path)
    with open(os.path.join(file_path, "url_from_robot_xml.json"), 'r') as file:
        data = json.load(file)
    start_urls = [x for x in data.keys()]
    # start_urls = [
    #     "https://www.mybenta.com/classified/396993/Boat+accessories++Trottle+Shift+Control",
    #     "https://www.mybenta.com/classified/396994/Acrylic+Product",
    # "https://www.mybenta.com/classified/396996/Ticketing+with+Bills+Payment+Package+with+Viaexpress",
    # "https://www.mybenta.com/classified/396997/Men%26%2339%3Bs+Nike+Hypershift+Basketball+Shoes+-+MENS+RUBBER+SHOES",
    # "https://www.mybenta.com/classified/396998/Boat+Steering+Helm+and+Bezel+brand+new",
    # "https://www.mybenta.com/classified/396999/Jinggong+JG80+Hydraulic+Excavator+%28.25+to+.30+m3%29+Chain-Type",
    # "https://www.mybenta.com/classified/397000/Jinggong+JG80+Hydraulic+Excavator+%28.25+to+.30+m3%29+Wheel-Type",
    # "https://www.mybenta.com/classified/397001/Nike+LeBron+Soldier+11+ELITE+-+BASKETBALL+SHOES",
    # "https://www.mybenta.com/classified/397002/Souvenir+and+Giveaways+Baby+Boy%26%2339%3Bs+Christening+%2F+Birthday+Mini+Shoe",
    # "https://www.mybenta.com/classified/397003/Souvenir+and+Giveaways+Cute+Shoes+for+baby+girl",
    # "https://www.mybenta.com/classified/397004/Souvenir+and+Giveaways+Chunky+Feeding+Bottle+for+souvenir",
    # "https://www.mybenta.com/classified/397005/Paul+George+SHOES+-+PG+1+SHOES+-+BASKETBALL+SHOES",
    # "https://www.mybenta.com/classified/397007/Acrylic+Poster",
    # ]
    result = {}
    # custom_settings = {
    #     "FEED_FORMAT": "json",
    #     "FEED_EXPORT_INDENT": 4,
    #     "FEED_URI": f"{name}.json",
    #     "DUPEFILTER_CLASS": "scrapy.dupefilters.RFPDupeFilter",
    # }
    def start_requests(self):
        for url in self.start_urls:
            yield scrapy.Request(url, callback=self.parse)

    def parse(self, response):
        cell = response.xpath("//div[@class='uk-grid uk-grid-small post-background uk-margin-small-left']/div[@class='panel-white post-white post panel-shadow uk-width-5-10 uk-margin-left uk-margin-top']/div[@class='post-description uk-margin-top']/p[@class='uk-margin-top']/span[@class='uk-text-bold uk-text-large uk-text-primary']/text()").get()
        if cell is not None or cell != "":
            self.result[cell] = len(self.result)

    def closed(self, reason):
        # Save the accumulated data to a JSON file when the spider is closed
        self.logger.info(f"Saving data to {self.name}.json")
        file_path = os.path.join(cwd,'output', self.name)
        if not os.path.exists(file_path):
            os.makedirs(file_path)

        with open(os.path.join(file_path, f"{self.name}.json"), 'w') as file:
            json.dump(self.result, file, indent=2)


def start_crawl(urls):
    process = CrawlerProcess(settings={
        # 'LOG_LEVEL': 'INFO',
        # "FEED_FORMAT": "json",
        # "FEED_EXPORT_INDENT": 4,
        # "FEED_URI": f"{UrlFromRobotXmlSpider.name}.json",
        # "DUPEFILTER_CLASS":"scrapy.dupefilters.RFPDupeFilter"
        # You can set the log level as per your requirement
    })
    process.crawl(CellNoSpider)
    process.start()

if __name__ == "__main__":

    file_path = os.path.join('output', 'url_from_robot_xml')
    if not os.path.exists(file_path):
        os.makedirs(file_path)

    # with open(os.path.join(file_path, "url_from_robot_xml.json"), 'r') as file:
    #     data = json.load(file)
    # all_urls = [x for x in data.keys()]
    all_urls = {
        "https://www.mybenta.com/classified/396993/Boat+accessories++Trottle+Shift+Control": 27298,
    "https://www.mybenta.com/classified/396994/Acrylic+Product": 27299,
    "https://www.mybenta.com/classified/396996/Ticketing+with+Bills+Payment+Package+with+Viaexpress": 27300,
    "https://www.mybenta.com/classified/396997/Men%26%2339%3Bs+Nike+Hypershift+Basketball+Shoes+-+MENS+RUBBER+SHOES": 27301,
    "https://www.mybenta.com/classified/396998/Boat+Steering+Helm+and+Bezel+brand+new": 27302,
    "https://www.mybenta.com/classified/396999/Jinggong+JG80+Hydraulic+Excavator+%28.25+to+.30+m3%29+Chain-Type": 27303,
    "https://www.mybenta.com/classified/397000/Jinggong+JG80+Hydraulic+Excavator+%28.25+to+.30+m3%29+Wheel-Type": 27304,
    "https://www.mybenta.com/classified/397001/Nike+LeBron+Soldier+11+ELITE+-+BASKETBALL+SHOES": 27305,
    "https://www.mybenta.com/classified/397002/Souvenir+and+Giveaways+Baby+Boy%26%2339%3Bs+Christening+%2F+Birthday+Mini+Shoe": 27306,
    "https://www.mybenta.com/classified/397003/Souvenir+and+Giveaways+Cute+Shoes+for+baby+girl": 27307,
    "https://www.mybenta.com/classified/397004/Souvenir+and+Giveaways+Chunky+Feeding+Bottle+for+souvenir": 27308,
    "https://www.mybenta.com/classified/397005/Paul+George+SHOES+-+PG+1+SHOES+-+BASKETBALL+SHOES": 27309,
    "https://www.mybenta.com/classified/397007/Acrylic+Poster": 27310,
    "https://www.mybenta.com/classified/397008/Nike+Kyrie+3+MENS+Basketball+Shoes+-+BASKETBALL+SHOES": 27311,
    "https://www.mybenta.com/classified/397009/Nike+Kyrie+3+MENS+Basketball+Shoes+-+BASKETBALL+SHOES": 27312,
    "https://www.mybenta.com/classified/397010/Nike+Zoom+KD+9+Elite+Men%26%2339%3Bs+Basketball+Shoes+-+RUBBER+SHOES": 27313,
    "https://www.mybenta.com/classified/397011/WORTH+P30%2C+000+FOR+TICKETING+FRANCHISE+PACKAGE": 27314,
    "https://www.mybenta.com/classified/397012/WORTH+P60%2C+000+FOR+TICKETING+W%2F+BILLS+PAYMENT+PACKAGE": 27315,
    "https://www.mybenta.com/classified/397015/WORTH+P175%2C+000+FOR+ALL+IN+ONE+FRANCHISE+PACKAGE": 27316,
    "https://www.mybenta.com/classified/397016/WORTH+P300%2C+000+FOR+PREMIUM+FRANCHISE+PACKAGE": 27317,
    "https://www.mybenta.com/classified/397017/Nike+Zoom+KD+9+Elite+Men%26%2339%3Bs+Basketball+Shoes+-+RUBBER+SHOES": 27318,

    }
    all_urls  = [str(x).strip() for x in all_urls.keys()]
    print(f"len of all_urls is {len(all_urls)}")

    batch_size = 50
    for i in range(0, len(all_urls), batch_size):
        batch_urls = all_urls[i:i + batch_size]
        p = Process(target=start_crawl, args=(batch_urls))
        p.start()
