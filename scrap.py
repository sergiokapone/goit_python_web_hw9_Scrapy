import os
import scrapy
import json
from scrapy.crawler import CrawlerProcess


class QuotesSpider(scrapy.Spider):
    name = "quotes"
    allowed_domains = ["quotes.toscrape.com"]
    start_urls = ["http://quotes.toscrape.com/"]
    custom_settings = {
        "FEED_FORMAT": "json",
        "FEED_URI": "quotes.json",
        "FEED_EXPORT_INDENT": 2,
    }

    all_quotes = []

    def parse(self, response):
        for quote in response.xpath("/html//div[@class='quote']"):
            quote_data = {
                "author": quote.xpath("span/small/text()").get(),
                "tags": quote.xpath("div[@class='tags']/a/text()").extract(),
                "quote": quote.xpath("span[@class='text']/text()").get(),
            }
            self.all_quotes.append(quote_data)

        next_link = response.xpath("//li[@class='next']/a/@href").get()
        if next_link:
            yield scrapy.Request(
                url=self.start_urls[0] + next_link, callback=self.parse
            )

    def closed(self, reason):

        data = self.all_quotes
        with open("quotes.json", "w") as file:
            json.dump(data, file, indent=2)


class AuthorsSpider(scrapy.Spider):
    name = "authors"
    allowed_domains = ["quotes.toscrape.com"]
    start_urls = ["http://quotes.toscrape.com/"]
    custom_settings = {
        "FEED_FORMAT": "json",
        "FEED_URI": "authors.json",
        "FEED_EXPORT_INDENT": 2,
    }

    def parse(self, response):
        # Витягування посилань на сторінки авторів із поточної сторінки
        author_links = response.css(".author + a::attr(href)").getall()
        yield from response.follow_all(author_links, self.parse_author)

        next_page = response.css("li.next a::attr(href)").get()
        if next_page:
            yield response.follow(next_page, self.parse)

    def parse_author(self, response):
        fullname = response.css(".author-title::text").get().strip().replace("-", " ")
        born_date = response.css(".author-born-date::text").get().strip()
        born_location = response.css(".author-born-location::text").get().strip()
        description = (
            response.css(".author-description::text").get().strip().replace('"', "")
        )

        author_data = {
            "fullname": fullname,
            "born_date": born_date,
            "born_location": born_location,
            "description": description,
        }

        yield author_data


if __name__ == "__main__":

    if os.path.exists("authors.json"):
        os.remove("authors.json")

    # Запуск павуків
    process = CrawlerProcess()
    process.crawl(QuotesSpider)
    process.crawl(AuthorsSpider)
    process.start()
