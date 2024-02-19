import scrapy
import pandas as pd
from scrapy.selector import Selector
from scrapy.exceptions import CloseSpider
from pydispatch.dispatcher import connect

"""
Columns for DataFrame:
    url
    name,
    course pesenter,
    platform,
    language,
    level,
    course duration,
    upcoming dates,
    finished dates,
    categories,
    cert price,
    subtitles
    
"""
class CourseSpider(scrapy.Spider):
    name = 'classcentral'
    def __init__(self, *args, **kwargs):
        super(CourseSpider, self).__init__(*args, **kwargs)
        connect(self.quit, scrapy.signals.spider_closed)
        self.cols = [
            'name',
            'presenter',
            'platform',
            'language',
            'level',
            'duration',
            'url',
            'certificate_price',
            'categories',
            'subtitles']
        self.data = pd.DataFrame(columns=self.cols)

    def start_requests(self):
        urls = ['https://www.classcentral.com/subjects']

        for url in urls:
            yield scrapy.Request(url=url, callback=self.init_parse)

    def quit(self, spider,):
        self.data.to_csv(f"data.csv", sep=';')

    def init_parse(self, response):
        curr_url = response.url
        subjects_href = response.xpath(f"""//*[@id="page-subjects"]/div[1]/section[2]/ul/li/h3/a[1]/@href""").getall()

        for url in subjects_href:
            yield response.follow(url, callback=self.subjects_courses_parser)

    def subjects_courses_parser(self, response):
        curr_url = response.url
        temp = response.xpath(f"""//div[@class="catalog-header"]/div/div/button[text()="Share"]/following-sibling::span/text()""").getall()
        num = temp[0].replace(',','').split(' ')[0]
        num = int(num)
        for page in range(num // 15):
        #for page in [0,]:
            yield response.follow(f"{curr_url}/?page={page+1}", callback=self.course_parser)


    def course_parser(self, response):
        lis = response.xpath(f"""//*[@id="page-subject"]/div[1]/div[3]/div[5]/ol/li""").getall()
        for l in lis:
            temp = Selector(text=l)
            if len(temp.xpath("""/li/iframe""").getall()) > 1:
                continue
            href = temp.xpath(f"""//li/div[1]/div[1]/div[2]/a[1]/@href""").getall()
            universities = temp.xpath(f"""//li/div[1]/div[1]/div[2]/span/span/a/@href""").getall()

            if len(href) > 0:
                yield response.follow(href[0], callback=self.details, cb_kwargs={'providers': universities})


    def get_cert_price(self, certificate: str) -> int:
        cert_price = 0
        if certificate:
            p = certificate.strip().split(' Certificate Available')[0]
            if '$' in p:
                cert_price = float(p[1:])
            elif 'Paid' in p:
                cert_price = None
        return cert_price

    def details(self, response, providers):
        language =        response.xpath(f"""//*[@id="page-course"]/div[1]/main/aside/div/ul/li/a[contains(@href,"/language/")]/text()""").get()
        level =           response.xpath(f"""//*[@id="page-course"]/div[1]/main/aside/div/ul/li/i[contains(@class,"icon-level-charcoal")]/following-sibling::span/text()""").get()
        certificate =     response.xpath(f"""//*[@id="page-course"]/div[1]/main/aside/div/ul/li/i[contains(@class,"icon-credential-charcoal")]/following-sibling::span/text()""").get()
        course_duration = response.xpath(f"""//*[@id="page-course"]/div[1]/main/aside/div/ul/li/i[contains(@class,"icon-clock-charcoal")]/following-sibling::span/text()""").get()
        platform =        response.xpath(f"""//*[@id="page-course"]/div[1]/main/aside/div/ul/li/a[contains(@href,"/provider/")]/text()""").get()
        name =            response.xpath(f"""//*[@id="page-course"]/div[1]/main/header/h1/text()""").get()
        cats =            response.xpath(f"""//*[@id="page-course"]/div[1]/main/header/div/nav/div/ul/li/a/span/text()""").getall()
        subs =            response.xpath(f"""//*[@id="page-course"]/div[1]/main/aside/div/ul/li/span/i[contains(@class,"icon-subtitles-charcoal")]/../following-sibling::span/text()""").get()
        
        subtitles = [sub.strip() for sub in subs.split(',')] if subs else []
        cert_price = self.get_cert_price(certificate)
        language = language.strip() if language else None
        level = level.strip() if level else None
        course_duration = course_duration.strip() if course_duration else None
        platform = platform.strip() if platform else None
        name = name.strip() if name else None
        categories = [cat.strip() for cat in cats] if cats else []
        '''
        url
        name,
        course pesenter,
        platform,
        language,
        level,
        course duration,
        certificate_price,
        categories
        subtitles
        '''
        new_data= {
            'name': name,
            'presenter': [providers],
            'platform': platform,
            'language': language,
            'level': level,
            'duration': course_duration,
            'url': response.url,
            'certificate_price': cert_price,
            'categories': [categories],
            'subtitles': [subtitles]
        }
        try:
            new_data = pd.DataFrame.from_dict(new_data, orient='columns',)
            self.data = self.data.append(new_data)
        except Exception as ex:
            print(f"error\t\tat: {response.url} for: {name}\n")
            raise CloseSpider(ex)
