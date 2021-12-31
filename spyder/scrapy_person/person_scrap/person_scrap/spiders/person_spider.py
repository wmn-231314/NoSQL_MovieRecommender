import re
import redis
import pymongo
import scrapy

redis_conn = redis.Redis(host='localhost', port=6379, db=0)


class QuotesSpider(scrapy.Spider):
    name = "quotes"

    def start_requests(self):

        redis_conn = redis.Redis(host='localhost', port=6379, db=0)
        wobu = redis_conn.smembers('2')
        urls=[]
        # See PyCharm help at https://www.jetbrains.com/help/pycharm/
        for i in wobu:
            print()
            urls.append('https://www.themoviedb.org/person/' + str(i, encoding='utf-8'))

        for url in urls:
            yield scrapy.Request(url=url, callback=self.parse)

    def parse(self, response):
        person_id = response.url.split('/')[-1]
        person_id = re.sub(r'\D', "", person_id)

        biography =  response.css("div.biography div.content div.text p::text").get()
        name =response.css("h2.title a::text ").get()
        gender=response.css("section.full_wrapper section p::text")[2].get()
        birth = response.css("section.full_wrapper section p::text")[4].get()
        place_birth = response.css("section.full_wrapper section p::text")[5].get()

        myclient = pymongo.MongoClient("mongodb://localhost:27017/")

        mydb = myclient["tmdb"]
        mycol = mydb["person"]
        mylist = {"person_id": person_id, "biography": biography, "name": name,"gender":gender,"birth":birth,"place_birth":place_birth}
        x = mycol.insert_one(mylist)

        # print list of the _id values of the inserted documents:
