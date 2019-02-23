# -*- coding: utf-8 -*-

# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://doc.scrapy.org/en/latest/topics/item-pipeline.html
import os
import json
import pymysql
import requests
from scrapy import Request
from fake_useragent import UserAgent
from .items import BaseMovie, BaseActor, RMRelation, MPRelation, BaseComment


class WuyanspiderPipeline(object):
    def __init__(self, user, password, host, port, database, pic_path):
        # self.db = db
        # self.cursor = self.db.cursor()
        self.user = user
        self.password = password
        self.host = host
        self.port = port
        self.database = database
        self.pic_path = pic_path

    @classmethod
    def from_crawler(cls, crawler):
        user = crawler.settings.get('MYSQL_USER')
        password = crawler.settings.get('MYSQL_PASSWORD')
        host = crawler.settings.get('MYSQL_HOST')
        port = crawler.settings.get('MYSQL_PORT')
        database = crawler.settings.get('MYSQL_DATABASE')
        pic_path = crawler.settings.get('PIC_DIR')
        return cls(user, password, host, port, database, pic_path)

    def open_spider(self, spider):
        self.db = pymysql.connect(user=self.user, password=self.password, host=self.host, port=self.port,
                                  database=self.database, charset='utf8', use_unicode=True)
        self.cursor = self.db.cursor()
        self.cursor.execute('SHOW DATABASES')
        databases = self.cursor.fetchall()
        if ('spider',) not in databases:
            self.cursor.execute('CREATE DATABASE `spider`')
        if ('spider_comment',) not in databases:
            self.cursor.execute('CREATE DATABASE `spider_comment`')

    def close_spider(self, spider):
        self.db.close()

    def process_item(self, item, spider):
        if isinstance(item, BaseMovie):
            self.handle_movie(item)
        elif isinstance(item, BaseActor):
            self.handle_actor(item)
        elif isinstance(item, RMRelation):
            self.handle_rmr(item)
        elif isinstance(item, MPRelation):
            self.handle_mpr(item)
        elif isinstance(item, BaseComment):
            self.handle_comment(item)
        else:
            pass
        return item

    def handle_movie(self, item):
        """
        处理电影的信息
        :param item:
        :return:
        """
        item = self.pic_save(item, 1)
        self.cursor.execute('SHOW TABLES')
        all_table = self.cursor.fetchall()
        if ('spider_movie',) not in all_table:
            try:
                self.cursor.execute('''CREATE TABLE `spider_movie`(
                `movie_name` CHAR(30) NOT NULL ,
                `long` VARCHAR(15),
                `rank` FLOAT,
                `star_num` VARCHAR(15),
                `director` TEXT,
                `main_actors` TEXT,
                `writer` TEXT,
                `year` VARCHAR(200) ,
                `class` VARCHAR(20),
                `countries` VARCHAR(20),
                `id` CHAR(15) PRIMARY KEY NOT NULL ,
                `review` TEXT,
                `details` TEXT,
                `poster` VARCHAR(50),
                `pic_path` TEXT)''')
            except Exception as e:
                print(e)

        if not self.cursor.execute('SELECT `name` FROM `spider_movie` WHERE id=%s', item['_id']):
            self.cursor.execute('''
            INSERT INTO `spider_movie`(
            `movie_name`,
            `long`,
            `rank`,
            `star_num`,
            `director`,
            `main_actors`,
            `writer`,
            `year`,
            `class`,
            `countries`,
            `id`,
            `review`,
            `details`,
            `poster`,
            `pic_path`
            ) VALUES(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ''', [
                item['movie_name'],
                item['long'],
                item['rank'],
                item['star_num'],
                item['director'],
                item['main_actor'],
                item['writer'],
                item['year'],
                item['_class'],
                item['countries'],
                item['_id'],
                item['review'],
                item['details'],
                item['poster'],
                item['image']
            ])
            self.db.commit()

    def handle_actor(self, item):
        """
        处理演员的信息
        :param item:
        :return:
        """
        item = self.pic_save(item, 2)
        self.cursor.execute('SHOW TABLES')
        all_table = self.cursor.fetchall()
        if ('spider_person',) not in all_table:
            try:
                self.cursor.execute('''CREATE TABLE `spider_person`(
                        `id` CHAR(15) PRIMARY KEY NOT NULL ,
                        `name` CHAR(50) NOT NULL ,
                        `sex` VARCHAR(5),
                        `constellation` VARCHAR(10),
                        `birthday` VARCHAR(30),
                        `birthplace` VARCHAR(50) ,
                        `profession` VARCHAR(60),
                        `imdb` VARCHAR(15),
                        `introduce` TEXT,
                        `poster` VARCHAR(50),
                        `image` TEXT
                        )''')
            except Exception as e:
                print(e)
        if not self.cursor.execute('SELECT `name` FROM `spider_person` WHERE id=%s', item['_id']):
            self.cursor.execute(
                '''
                INSERT INTO `spider_person`(
                `id`, 
                `name`, 
                `sex`,
                `constellation`,
                `birthday`,
                `birthplace`,
                `profession`,
                `imdb`,
                `introduce`,
                `poster`,
                `image`) VALUES(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ''', [
                    item['_id'],
                    item['name'],
                    item['sex'],
                    item['constellation'],
                    item['birthday'],
                    item['birthplace'],
                    item['profession'],
                    item['imdb'],
                    item['introduce'],
                    item['poster'],
                    item['image']
                ]
            )
            self.db.commit()

    def handle_rmr(self, item):
        self.cursor.execute('SHOW TABLES')
        all_table = self.cursor.fetchall()
        _type = item.pop('rank_type')
        if (_type, ) not in all_table:
            try:
                self.cursor.execute('''
                CREATE TABLE %s(
                `rank` INTEGER NOT NULL PRIMARY KEY,
                `movie_id` VARCHAR(15) NOT NULL
                )
                ''', (_type,))
            except Exception as e:
                print(e)

        if not self.cursor.execute('SELECT `rank` FROM '+'`'+_type+'`'+' WHERE movie_id=%s', [item['movie_id'],]):
            sql = 'INSERT INTO '+'`'+_type+'`'
            self.cursor.execute(sql+'(`rank`, `movie_id`) VALUES(%s, %s)', [
                item['rank'],
                item['movie_id']
            ])
            self.db.commit()

    def handle_mpr(self, item):
        """
        1是导演，2是编剧，3是演员
        :param item:
        :return:
        """
        self.cursor.execute('use `spider`')
        self.cursor.execute('SHOW TABLES')
        all_table = self.cursor.fetchall()
        if ('spider_mpr',) not in all_table:
            try:
                self.cursor.execute('''
                CREATE TABLE `spider_mpr`(
                `type` VARCHAR(1),
                `movie_id` VARCHAR(15),
                `person_id` VARCHAR(30)
                )
                ''')
            except Exception as e:
                print(e)
        # 据说不用*会节省很多资源
        if not self.cursor.execute('SELECT `movie_id` FROM `spider_mpr` WHERE movie_id=%s and person_id=%s and type=%s', [item['movie_id'], item['person_id'], item['_type']]):
            self.cursor.execute('''
            INSERT INTO `spider_mpr`(`type`, `movie_id`, `person_id`) VALUES(%s, %s, %s)
            ''', [item['_type'], item['movie_id'], item['person_id']])
            self.db.commit()

    def handle_comment(self, item):
        item = self.pic_save(item, 3)
        self.cursor.execute('USE `spider_comment`')
        self.cursor.execute('SHOW TABLES')
        all_table = self.cursor.fetchall()
        name = 'spider_' + item['movie_id']
        if (item['movie_id'],) not in all_table or not all_table:
            try:
                s = 'CREATE TABLE '+'`'+item['movie_id']+'`'
                self.cursor.execute('''
                {}(
                `user_id` VARCHAR(30),
                `user_name` VARCHAR(30),
                `comment_time` DATETIME,
                `comment` TEXT,
                `image` VARCHAR(50)
                )
                '''.format(s))
            except Exception as e:
                print(e)

        if not self.cursor.execute('SELECT user_id FROM '+'`'+item['movie_id']+'`'+'WHERE user_id=%s', (item['user_id'],)):
            sql = 'INSERT INTO '+'`'+item['movie_id']+'`'
            self.cursor.execute(sql+'(`user_id`, `user_name`, `comment_time`, `comment`,`image`) VALUES(%s, %s, %s, %s, %s)', [item['user_id'], item['user_name'], item['comment_time'], item['comment'], item['image']])
            self.db.commit()

    def pic_save(self, item, _type):
        """
        _type取值1,2,3,4
        1：电影
        2：人物
        3：用户评论
        :param item:
        :param _type:
        :return:
        """
        fake = UserAgent()
        headers = {'User-Agent': fake.random}
        response = requests.get('https://movie.douban.com', headers=headers)
        cookie = response.cookies
        if _type == 1:
            poster_path = os.path.join('movie', item['_id'] + '.jpg')
            with open(os.path.join(self.pic_path, poster_path), 'wb') as f:
                f.write(requests.get(item['poster'], headers=headers, cookies=cookie).content)
                item['poster'] = poster_path
            pictures = []
            for n, pic_url in zip(range(item['image']), item['image']):
                pic = item['_id'] + '_' + str(n) + '.jpg'
                pic_path = os.path.join('movie', pic)
                pictures.append(pic_path)
                with open(os.path.join(self.pic_path, pic_path), 'wb') as f:
                    f.write(requests.get(pic_url, headers=headers, cookies=cookie).content)
            item['image'] = json.dumps(pictures)
            return item

        elif _type == 2:
            poster_path = os.path.join('person', item['_id'] + '.jpg')
            with open(os.path.join(self.pic_path, poster_path), 'wb') as f:
                f.write(requests.get(item['poster'], headers=headers, cookies=cookie).content)
                item['poster'] = poster_path
            pictures = []
            for n, pic_url in zip(range(item['image']), item['image']):
                pic = item['_id'] + '_' + str(n) + '.jpg'
                pic_path = os.path.join('people', pic)
                pictures.append(pic_path)
                with open(os.path.join(self.pic_path, pic_path), 'wb') as f:
                    f.write(requests.get(pic_url, headers=headers, cookies=cookie).content)
            item['image'] = json.dumps(pictures)
            return item

        elif _type == 3:
            pic_ = os.path.join('user', item['user_id'] + '.jpg')
            with open(os.path.join(self.pic_path, pic_), 'wb') as f:
                f.write(requests.get(item['image'], headers=headers, cookies=cookie).content)
                item['image'] = pic_
            return item
        else:
            raise Exception('Wrong Type of Picture')
