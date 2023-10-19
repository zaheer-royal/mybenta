import scrapy, json, os
import xmltodict
from scrapy.crawler import CrawlerProcess
from multiprocessing import Process
cwd = os.getcwd()

class UrlFromRobotXmlSpider(scrapy.Spider):
    name = "url_from_robot_xml"
    allowed_domains = ["mybenta.com"]
    # start_urls = [
    #     "https://www.mybenta.com/sellermap_50000.xml",
    #     "https://www.mybenta.com/sellermap_100000.xml",
    #     "https://mybenta.com/sitemap_classified_100000.xml",
    #     "https://mybenta.com/sitemap_classified_150000.xml",
    #     "https://mybenta.com/sitemap_classified_200000.xml",
    #     "https://mybenta.com/sitemap_classified_250000.xml",
    # ]
    custom_settings = {
        # "FEED_FORMAT": "json",
        # "FEED_EXPORT_INDENT": 4,
        # "FEED_URI": f"{name}.json",
        # "DUPEFILTER_CLASS": "scrapy.dupefilters.RFPDupeFilter",
    }

    # Class-level variable to store accumulated data
    result = {}

    def start_requests(self):
        for url in self.start_urls:
            yield scrapy.Request(url, callback=self.parse, headers={"Accept": "application/xml"})

    def parse(self, response):
        data = xmltodict.parse(response.body)
        urlset = data.get("urlset", {}).get("url", [])

        for k, item in enumerate(urlset):
            loc = str(item.get("loc", "")).strip()
            if loc == "":
                continue
            self.result[loc] = k

        # Yielding an item is not necessary for this use case
        # You can yield an item if needed for other purposes
        # yield {
        #     'result': self.result,
        # }

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
    process.crawl(UrlFromRobotXmlSpider, start_urls=urls)
    process.start()

if __name__ == "__main__":
    batch_size = 10

    file_path = os.path.join(cwd,'output', 'robot_text')
    if not os.path.exists(file_path):
        os.makedirs(file_path)

    with open(os.path.join(file_path, "robot_text.json"), 'r') as file:
        data = json.load(file)
    all_urls = [x for x in data.keys()]

    for i in range(0, len(all_urls), batch_size):
        batch_urls = all_urls[i:i + batch_size]
        p = Process(target=start_crawl, args=(batch_urls,))
        p.start()
