# -*- coding: utf-8 -*-

# Define here the models for your spider middleware
#
# See documentation in:
# https://doc.scrapy.org/en/latest/topics/spider-middleware.html
import requests
from scrapy import signals
from collections import defaultdict
from scrapy.http.cookies import CookieJar
from scrapy.downloadermiddlewares.useragent import UserAgentMiddleware
from scrapy.downloadermiddlewares.cookies import CookiesMiddleware
from fake_useragent import UserAgent
# import random


class MyCookiesMiddleware(CookiesMiddleware):

    def __init__(self, crawler, debug=False):
        self.jars = defaultdict(CookieJar)
        self.debug = debug
        self.crawler = crawler
        self.count = 0

    @classmethod
    def from_crawler(cls, crawler):
        # print(dir(crawler))
        if not crawler.settings.getbool('COOKIES_ENABLED'):
            pass
        return cls(crawler, crawler.settings.getbool('COOKIES_DEBUG'))

    def process_request(self, request, spider):
        if request.meta.get('dont_merge_cookies', False):
            return

        cookiejarkey = request.meta.get("cookiejar")
        jar = self.jars[cookiejarkey]
        cookies = self._get_request_cookies(jar, request)
        for cookie in cookies:
            jar.set_cookie_if_ok(cookie, request)

        # set Cookie header
        request.headers.pop('Cookie', None)
        c = 0 if not self.crawler.stats._stats.get('downloader/request_count') else self.crawler.stats._stats.get('downloader/request_count')
        if c - self.count > 50:
            self.count = c
            jar.add_cookie_header(request)
        self._debug_cookie(request, spider)


class MyUserAgentMiddleware(UserAgentMiddleware):
    """
    利用fake_useragent来设置useragent
    """
    def __init__(self, useragent):
        self.user_agent = useragent

    @classmethod
    def from_crawler(cls, crawler):
        fake = UserAgent()
        return cls(fake.random)

    def process_request(self, request, spider):
            agent = self.user_agent
            request.headers['User-Agent'] = agent


class WuyanspiderSpiderMiddleware(object):
    # Not all methods need to be defined. If a method is not defined,
    # scrapy acts as if the spider middleware does not modify the
    # passed objects.

    @classmethod
    def from_crawler(cls, crawler):
        # This method is used by Scrapy to create your spiders.
        s = cls()
        crawler.signals.connect(s.spider_opened, signal=signals.spider_opened)
        return s

    def process_spider_input(self, response, spider):
        # Called for each response that goes through the spider
        # middleware and into the spider.

        # Should return None or raise an exception.
        return None

    def process_spider_output(self, response, result, spider):
        # Called with the results returned from the Spider, after
        # it has processed the response.

        # Must return an iterable of Request, dict or Item objects.
        for i in result:
            yield i

    def process_spider_exception(self, response, exception, spider):
        # Called when a spider or process_spider_input() method
        # (from other spider middleware) raises an exception.

        # Should return either None or an iterable of Response, dict
        # or Item objects.
        pass

    def process_start_requests(self, start_requests, spider):
        # Called with the start requests of the spider, and works
        # similarly to the process_spider_output() method, except
        # that it doesn’t have a response associated.

        # Must return only requests (not items).
        for r in start_requests:
            yield r

    def spider_opened(self, spider):
        spider.logger.info('Spider opened: %s' % spider.name)


class WuyanspiderDownloaderMiddleware(object):
    # Not all methods need to be defined. If a method is not defined,
    # scrapy acts as if the downloader middleware does not modify the
    # passed objects.

    @classmethod
    def from_crawler(cls, crawler):
        # This method is used by Scrapy to create your spiders.
        s = cls()
        crawler.signals.connect(s.spider_opened, signal=signals.spider_opened)
        return s

    def process_request(self, request, spider):
        # Called for each request that goes through the downloader
        # middleware.

        # Must either:
        # - return None: continue processing this request
        # - or return a Response object
        # - or return a Request object
        # - or raise IgnoreRequest: process_exception() methods of
        #   installed downloader middleware will be called
        return None

    def process_response(self, request, response, spider):
        # Called with the response returned from the downloader.

        # Must either;
        # - return a Response object
        # - return a Request object
        # - or raise IgnoreRequest
        return response

    def process_exception(self, request, exception, spider):
        # Called when a download handler or a process_request()
        # (from other downloader middleware) raises an exception.

        # Must either:
        # - return None: continue processing this exception
        # - return a Response object: stops process_exception() chain
        # - return a Request object: stops process_exception() chain
        pass

    def spider_opened(self, spider):
        spider.logger.info('Spider opened: %s' % spider.name)
