# -*- coding: utf-8 -*-
import re
import json
import scrapy
from ..items import BaseMovie, BaseActor, BaseComment, RMRelation, MPRelation
from scrapy import Request


class ClassifySpider(scrapy.Spider):
    name = 'classify'
    allowed_domains = ['douban.com', 'doubanio.com']
    start_urls = ['https://movie.douban.com/chart']

    def parse(self, response):
        print(response.text)
        name = response.xpath('//*[@id="content"]/div/div[2]/div[1]/div/span/a')
        for part in name:
            p = part.re('type=(\\d+).*?>(.*?)<')
            print(p, part)
            url = 'https://movie.douban.com/j/chart/top_list?type={}&interval_id=100%3A90&action=&start=0&limit=250'.format(p[0])
            req = Request(url, callback=self.parse_type)
            req.meta['rank_type'] = p[1]
            yield req

    def parse_type(self, response):
        """
        1.每个分类电影的排名与电影的存储
        2.生成爬取电影详细信息的Request
        :param response:
        :return:
        """
        rank_type = response.meta['rank_type']
        j = json.loads(response.text)
        for i in j:
            name = i['title']
            rank = i['score']
            star_num = i['vote_count']
            director = None
            main_actor = i['actors']
            year = i['release_date']
            _class = i['types']
            countries = i['regions']
            _id = i['id']
            review = None
            rmr = RMRelation(rank_type=rank_type, rank=i['rank'], movie_id=_id)
            yield rmr
            movie = BaseMovie(
                name=name,
                rank=rank,
                star_num=star_num,
                director=None,
                main_actor=None,
                writer=None,
                year=year,
                _class=_class,
                _id=_id,
                review=review,
                details=None,
                poster=i['cover_url'],
                image=None
            )
            content_request = Request(i['url'], callback=self.movie_parse)
            content_request.meta['movie'] = movie
            yield content_request

    def movie_parse(self, response):
        """
        1.用来处理电影的信息
        2.生成处理电影演员的Request
        :param response:
        :return:
        """
        try:
            content = response.xpath('span[property="v:summary"]').extract()[0].strip()
        except Exception as e:
            content = response.xpath('span[class="all hidden"]').extract()[0].strip()
        response.meta['movie']['details'] = content
        # 对电影剧照的收集
        images = response.xpath('//*[@id="related-pic"]/ul/li')
        image = []
        for image_ in images:
            image.extend(image_.xpath('./a/img/@href').extract())
        response.meta['movie']['image'] = image
        # 对电影演员的收集，这些演员信息最终是要保存一部分到movie的原始信息中的，
        # 所以把movie的原始信息传过去
        # person_url = response.xpath('//*[@id="celebrities"]/h2/span/a/@href').extract()[0]
        person_url = 'https://movie.douban.com/subject/'+response.meta['movie']['_id']+'/celebrities'
        person_request = Request(person_url, callback=self.mpr_parse)
        person_request.meta['movie'] = response.meta['movie']
        yield person_request
        # 然后是对评论信息的收集，存储评论的时候需要电影ID，所以把电影ID传进去
        comment_url = response.xpath('//*[@id="comments-section"]/div[1]/h2/span/a/@href').extract()[0]
        comment_request = Request(comment_url, callback=self.comment_parse)
        comment_request.meta['movie_id'] = response.meta['movie']['_id']
        yield comment_request

    def mpr_parse(self, response):
        """
        1.用来生成名人与电影的关系，并存储
        2.生成处理名人个人信息的Request
        :param response:
        :return:
        """
        movie_id = response.meta['movie']['_id']
        # 寻找并分类演员，导演，编剧，然后分别储存为(人物名字，人物ID)的格式，以便以后的查询
        # 并要生成演员的爬取链接来爬取
        directors = response.xpath('//*[@id="celebrities"]/div[1]/ul/li')
        for director_ in directors:
            director_url = director_.xpath('./div/span[1]/a/@href').extract()[0]
            director_request = Request(director_url, callback=self.person_parse)
            person_id = re.findall('/(\\d+?)/', director_url)[0]
            yield MPRelation(movie_id=movie_id, person_id=person_id, _type=1)
            yield director_request

        actors = response.xpath('//*[@id="celebrities"]/div[2]/ul/li')
        for actor_ in actors:
            actor_url = actor_.xpath('./div/span[1]/a/@href').extract()[0]
            actor_request = Request(actor_url, callback=self.person_parse)
            person_id = re.findall('/(\\d+?)/', actor_url)[0]
            yield MPRelation(movie_id=movie_id, person_id=person_id, _type=3)
            yield actor_request

        writers = response.xpath('//*[@id="celebrities"]/div[3]/ul/li')
        for writer_ in writers:
            writer_url = writer_.xpath('./div/span[1]/a/@href').extract()[0]
            writer_request = Request(writer_url, callback=self.person_parse)
            person_id = re.findall('/(\\d+?)/', writer_url)[0]
            yield MPRelation(movie_id=movie_id, person_id=person_id, _type=2)
            yield writer_request

    def comment_parse(self, response):
        """
        用来解析每个电影热评的第一页
        :param response:
        :return:
        """
        movie_id = response.meta['movie_id']
        comments = re.findall('title="(.*?)".*?"https://www.douban.com/people/(.*?)/".*?src="(.*?)".*?<.*?comment-time.*?title="(.*?)".*?short">(.*?)<', response.text, re.S)
        for comment_ in comments:
            # [comment[1], comment[0], str(comment[3]).replace('\n', ''), '''{}'''.format(comment[4])]
            user_id = comment_[1]
            user_name = comment_[0]
            comm_time = str(comment_[3]).replace('\n', '')
            comm = comment_[4]
            image = re.sub('/u(.*?)-.*?\\.', '/ul\\1.', comment_[2])
            i = BaseComment(
                movie_id=movie_id,
                user_id=user_id,
                user_name=user_name,
                comment=comm,
                comment_time=comm_time,
                image=image
            )
            yield i

    def person_parse(self, response):
        """
        用来解析名人的个人信息
        :param response:
        :return:
        """
        info = response.text
        _id = re.findall('id="headline".*?rel="nofollow".*?https://movie.douban.com/celebrity/(\d*?)/', info, re.S)
        name = re.findall(r'<div id="content">.*?<h1>(.+)</h1>', info, re.S)[0]
        try:
            sex = re.findall(r'<span>性别<.+>:\s*(.*)\s*', info)[0]
        except:
            print('Can not find actor sex')
            sex = None
        try:
            constellation = re.findall(r'<span>星座<.+>:\s*(.*)\s*', info)[0]
        except:
            print('Can not find constellation')
            constellation = None
        try:
            birthday = re.findall(r'<span>出生日期<.+>:\s*(.*)\s*', info)[0]
        except Exception as e:
            try:
                birthday = re.findall(r'<span>生卒日期<.+>:\s*(.*)\s*', info)[0]
            except:
                print('Can not find birthday')
                birthday = None
        try:
            birthplace = re.findall(r'<span>出生地<.+>:\s*(.*)\s*', info)[0]
        except:
            print('Can not find birthplace')
            birthplace = None
        try:
            profession = re.findall(r'<span>职业<.+>:\s*(.*)\s*', info)[0]
        except:
            print('Can not find profession')
            profession = None
        try:
            imdb_number = re.findall(r'<span>imdb编号<.+>:\s*.+>(.+)</a>', info)[0]
        except:
            print('Can not find IMDB编号')
            imdb_number = None

        # 寻找照片
        poster = re.findall('', info, re.S)[0]
        image = re.findall('', info, re.S)

        all_introduce = re.findall(r'<span class="all hidden">\s*(.+)<', info)
        if not bool(all_introduce):
            normal_introduce = re.findall(r'<h2>\s*影人简介\s*.+\s*<.+>\s*</div>\s*<div class="bd">\s*(.+)\s*', info)
            person = BaseActor(_id=_id,
                               name=name,
                               sex=sex,
                               constellation=constellation,
                               birthday=birthday,
                               birthplace=birthplace,
                               profession=profession,
                               imdb=imdb_number,
                               introduce=normal_introduce[0],
                               poster=poster,
                               image=image)
        else:
            person = BaseActor(_id=_id,
                               name=name,
                               sex=sex,
                               constellation=constellation,
                               birthday=birthday,
                               birthplace=birthplace,
                               profession=profession,
                               imdb=imdb_number,
                               introduce=all_introduce[0],
                               poster=poster,
                               image=image)
        yield person
    #     final_request = Request(person['image'], callback=self.final_person_parse)
    #     final_request.meta['person'] = person
    #
    # def final_person_parse(self, response):
    #     pass
