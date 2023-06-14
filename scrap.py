import json
import scrapy
from itemadapter import ItemAdapter
from scrapy.crawler import CrawlerProcess
from scrapy.item import Item, Field
from database.models import Authors, Quotes
from database.connect import get_database
from mongoengine.errors import DoesNotExist


import logging
from scrapy.utils.log import configure_logging 

configure_logging(install_root_handler=False)
logging.disable(logging.DEBUG)


# Контейнери даних

class QuoteItem(Item):
    tags = Field()
    author = Field()
    quote = Field()


class AuthorItem(Item):
    fullname = Field()
    born_date = Field()
    born_location = Field()
    description = Field()

# Павук

class MainSpider(scrapy.Spider):
    name = "main_spider"
    allowed_domains = ["quotes.toscrape.com"]
    start_urls = ["http://quotes.toscrape.com"]
    custom_settings = {
        'ITEM_PIPELINES': {'__main__.MainPipeline': 100},
        'MONGODB_COLLECTION': 'quotes',
        'MONGODB_UNIQUE_KEY': 'quote',
        'MONGODB_ADD_TIMESTAMP': True
    }


    def parse(self, response, *args):

        # Queries

        QUERY_QUOTE = "/html//div[@class='quote']"
        QUERY_TAG = "div[@class='tags']/a[@class='tag']/text()"
        QUERY_AUTHOR = "span/small[@class='author']/text()"
        QUERY_TEXT = "span[@class='text']/text()"
        AUTHOR_LINK = "span/a/@href"
        BUTTON_NEXT = "//li[@class='next']/a/@href"

        for el in response.xpath(QUERY_QUOTE):
            tags = [e.strip() for e in el.xpath(QUERY_TAG).extract()]
            author = el.xpath(QUERY_AUTHOR).get().strip()\
                .replace('-', " ") # 6'th page problem
            quote = el.xpath(QUERY_TEXT).get()\
                .strip().replace('“', "").replace('”', "")

            yield  QuoteItem(tags=tags,
                             author=author,
                             quote=quote)

            yield response.follow(
                url=self.start_urls[0] + el.xpath(AUTHOR_LINK).get().strip(),
                callback=self.parse_author
            )

        next_link = response.xpath(BUTTON_NEXT).get()
        if next_link:
            yield scrapy.Request(url=self.start_urls[0] + next_link.strip())

    def parse_author(self, response, *args):

        # Queries

        AUTHOR_DETAILS = "/html//div[@class='author-details']"
        FULLNAME = "h3[@class='author-title']/text()"
        BORN_DATE = "p/span[@class='author-born-date']/text()"
        BORN_LOCATION = "p/span[@class='author-born-location']/text()"
        DESCRIPTION = "div[@class='author-description']/text()"

        content = response.xpath(AUTHOR_DETAILS)
        fullname = content.xpath(FULLNAME).get().strip().strip()\
            .replace('-', " ") # 6'th page problem
        born_date = content.xpath(BORN_DATE).get().strip()
        born_location = content.xpath(BORN_LOCATION).get().strip()
        description = content.xpath(DESCRIPTION).get().strip()

        yield AuthorItem(fullname=fullname,
                         born_date=born_date,
                         born_location=born_location,
                         description=description)


class MainPipeline:
    authors = []
    quotes = []
    def process_item(self, item, spider):
        adapter = ItemAdapter(item)
        data = adapter.asdict() 
        if isinstance(item, AuthorItem):
            try:
                author = Authors.objects.get(fullname=adapter['fullname'])
                self.authors.append(data)
            except DoesNotExist:
                author = Authors(**data)
                author.save()
        elif isinstance(item, QuoteItem):
            try:
                author = Authors.objects.get(fullname=adapter['author'])
            except DoesNotExist:
                author = Authors(fullname=adapter['author'])
                author.save()
            quote = Quotes(
                author=author,
                tags=adapter['tags'],
                quote=adapter['quote']
            )
            quote.save()
            self.quotes.append(adapter.asdict())
        return item
    
    def close_spider(self, spider):

        with open('quotes.json', 'w', encoding='utf-8') as file_quotes:
            json.dump(self.quotes, 
                      file_quotes, 
                      ensure_ascii=False, 
                      indent=2)

        with open('authors.json', 'w', encoding='utf-8') as file_authors:
            json.dump(self.authors, 
                      file_authors, 
                      ensure_ascii=False, 
                      indent=2)



if __name__ == '__main__':
    get_database()

    # Cclean base
    Authors.objects.all().delete()
    Quotes.objects.all().delete()

    process = CrawlerProcess()
    process.crawl(MainSpider)
    process.start()
    logging.info('End')
