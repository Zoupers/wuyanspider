# -*- coding: utf-8 -*-

# Define here the models for your scraped items
#
# See documentation in:
# https://doc.scrapy.org/en/latest/topics/items.html

import scrapy


class WuyanspiderItem(scrapy.Item):
    # define the fields for your item here like:
    # name = scrapy.Field()
    pass


class BaseMovie(scrapy.Item):
    name = scrapy.Field()
    long = scrapy.Field()
    rank = scrapy.Field()
    star_num = scrapy.Field()
    director = scrapy.Field()
    main_actor = scrapy.Field()
    writer = scrapy.Field()
    year = scrapy.Field()
    _class = scrapy.Field()
    countries = scrapy.Field()
    _id = scrapy.Field()
    review = scrapy.Field()
    details = scrapy.Field()
    # 在初始化的时候只存地址，在经过pipeline后再提出，然后赋值为正确的值
    poster = scrapy.Field()
    image = scrapy.Field()


class BaseActor(scrapy.Item):
    _id = scrapy.Field()
    name = scrapy.Field()
    sex = scrapy.Field()
    constellation = scrapy.Field()
    birthday = scrapy.Field()
    birthplace = scrapy.Field()
    profession = scrapy.Field()
    imdb = scrapy.Field()
    introduce = scrapy.Field()
    # 在初始化的时候只存地址，在经过pipeline后再提出，然后赋值为正确的值
    poster = scrapy.Field()
    image = scrapy.Field()


class RMRelation(scrapy.Item):
    """
    排行榜，电影关系
    """
    rank_type = scrapy.Field()
    rank = scrapy.Field()
    movie_id = scrapy.Field()


class MPRelation(scrapy.Item):
    """
    电影，人物关系
    """
    movie_id = scrapy.Field()
    person_id = scrapy.Field()
    # 1是导演，2是编剧，3是演员
    _type = scrapy.Field()


class BaseComment(scrapy.Item):
    """
    电影评论
    """
    movie_id = scrapy.Field()
    user_id = scrapy.Field()
    user_name = scrapy.Field()
    comment_time = scrapy.Field()
    comment = scrapy.Field()
    # 在初始化的时候只存地址，在经过pipeline后再提出，然后赋值为正确的值
    image = scrapy.Field()
