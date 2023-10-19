import scrapy, re, logging, json


class RobotTextSpider(scrapy.Spider):
    name = "robot_text"
    allowed_domains = ["mybenta.com"]
    start_urls = ["http://mybenta.com/robots.txt"]

    def parse(self, response):
        res = str(response.text)
        res = res.replace("Sitemap:",",")
        res = {v.strip():k for k,v in enumerate(res.split(",")) if v}
        file_path = os.path.join('output',self.name)
        if not os.path.exists(file_path):
            os.makedirs(file_path)

        with open(os.path.join(file_path,f"{self.name}.json"),'w') as file:
            json.dump(res,file,indent=2)
        return res

