# -*- coding: utf-8 -*-
import json
import scrapy
from scrapy import Request
from ..items import BaseMovie, BaseActor, BaseComment, RMRelation, MPRelation
from .classify import ClassifySpider


class Top250Spider(scrapy.Spider):
    name = 'top250'
    allowed_domains = ['douban.com']
    start_urls = ['http://movie.douban.com/top250']

    def parse(self, response):
        # 为了使用之前已经在classify的爬虫中定义的解析电影方法
        self.parse_source = ClassifySpider()
        next_page_url = response.xpath('//*[@id="content"]/div/div[1]/div[2]/span[3]/a/@href').extract()
        if next_page_url:
            yield Request(next_page_url[0], callback=self.parse)
        movies = response.xpath('//*[@id="content"]/div/div[1]/ol/li')
        for movie_ in movies:
            rank_rank = movie_.xpath('./div/div[1]/em').extract()[0]
            name = movie_.xpath('./div/div[2]/div[1]/a/span[1]/text()').extract()[0]
            long = None
            rank = None
            star_num = None
            director = None
            main_actor = None
            writer = None
            year = movie_.xpath().extract()[0]
            _class = movie_.xpath().extract()[0]
            countries = movie_.xpath().extract()[0]
            _id = movie_.xpath('./div/div[2]/div[1]/a/@href').extract()[0].split('/')[-2]
            review = movie_.xpath().extract()[0]
            details = None
            poster = movie_.xpath().extract()[0]
            image = None

            movie_url = movie_.xpath().extract()[0]

            rmr = RMRelation(_type='top250', rank=rank_rank, movie_id=_id)
            yield rmr
            movie = BaseMovie(
                name=name,
                long=long,
                rank=rank,
                star_num=star_num,
                director=director,
                main_actor=main_actor,
                writer=writer,
                year=year,
                _class=_class,
                countries=countries,
                _id=_id,
                review=review,
                details=details,
                poster=poster,
                image=image
            )
            movie_request = Request(movie_url, callback=self.movie_parse)
            movie_request.meta['movie'] = movie
            yield movie_request

    def movie_parse(self, response):
        """
        单独对top250榜单的电影进行处理
        :param response:
        :return:
        """
        _all = response.xpath('/html/head/script[19]/text()').extract()[0]
        total = json.loads(_all.replace('\n', ''))
        # rank, star_num, director, main_actor, writer
        long = response.xpath('//*[@id="info"]/span[@property="v:runtime"]/text()').extract()[0]
        response.meta['movie']['long'] = long
        try:
            content = response.xpath('span[property="v:summary"]').extract()[0].strip()
        except Exception as e:
            content = response.xpath('span[class="all hidden"]').extract()[0].strip()
        response.meta['movie']['details'] = content
        # 对电影剧照的收集
        images = response.xpath('//*[@id="related-pic"]/ul/li')
        image = []
        for image_ in images:
            img = image_.xpath('./a/img/@href').extract()
            image.extend(img)
        response.meta['movie']['image'] = image
        # 对电影演员的收集，这些演员信息最终是要保存一部分到movie的原始信息中的，
        # 所以把movie的原始信息传过去
        # person_url = response.xpath('//*[@id="celebrities"]/h2/span/a/@href').extract()[0]
        person_url = 'https://movie.douban.com/subject/'+response.meta['movie']['movie_id']+'/celebrities'
        person_request = Request(person_url, callback=self.parse_source.mpr_parse)
        person_request.meta['movie'] = response.meta['movie']
        yield person_url
        # 然后是对评论信息的收集，存储评论的时候需要电影ID，所以把电影ID传进去
        comment_url = response.xpath('//*[@id="comments-section"]/div[1]/h2/span/a/@href').extract()[0]
        comment_request = Request(comment_url, callback=self.parse_source.comment_parse)
        comment_request.meta['movie_id'] = response.meta['movie']['_id']
        yield comment_request
