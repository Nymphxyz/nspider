import re
import nspider

base_url = 'https://tieba.baidu.com'

class PageContentParser(nspider.Parser):
    def parse(self, request, response):
        is_failed = False

        text = nspider.common.get_text_from_res(response)
        selector = nspider.Selector(text)
        content = selector.soup.find("div", attrs={"class": "p_postlist"})
        if content:
            imgs = content.find_all("img", attrs={"class": "BDE_Image"})
            if imgs:
                for img in imgs:
                    if img.get("ad-dom-img"):
                        pass
                    else:
                        self.resource(img.get("src"), save_dir="./test-download", dupe_filter=True)
            next_page = selector.xpath('//li[contains(@class, "l_pager")]/a[text()="下一页"]/@href')
            if next_page:
                self.request(base_url + next_page[0], parser_class=PageContentParser)
        else:
            is_failed = True
            self.logger.info("URL: {} parser failed".format(request.url))

        return None, is_failed

    def pipeline(self, obj):
        pass


class ArticleListParser(nspider.Parser):

    def parse(self, request, response):
        is_failed = False

        text = nspider.common.get_text_from_res(response)

        bs64_str = re.findall('<code class="pagelet_html" id="pagelet_html_frs-list/pagelet/thread_list" style="display:none;">[.\n\S\s]*?</code>',text)
        bs64_str = ''.join(bs64_str).replace( '<code class="pagelet_html" id="pagelet_html_frs-list/pagelet/thread_list" style="display:none;"><!--', '')
        bs64_str = bs64_str.replace('--></code>', '')
        # print(text)
        if bs64_str:
            selector = nspider.Selector(bs64_str)

            # 标题列表
            title_list = selector.xpath('//div[@class="threadlist_title pull_left j_th_tit "]/a[1]/@title')
            # 链接列表
            link_list = selector.xpath('//div[@class="threadlist_title pull_left j_th_tit "]/a[1]/@href')
            # next page
            next_page = selector.xpath('//a[@class="next pagination-item "]/@href')

            self.logger.info(title_list)

            if next_page:
                self.request("https:" + next_page[0], parser_class=ArticleListParser)

            for link in link_list:
                self.request(base_url + link, parser_class=PageContentParser)

        else:
            is_failed = True
            self.logger.info("URL: {} parser failed".format(request.url))
        return None, is_failed

    def pipeline(self, obj):
        pass

proxy = {"https": "47.242.78.34:8000"}
class WangBingbingSpider(nspider.Spider):
    #start_url = ["https://tieba.baidu.com/f?kw=%E7%8E%8B%E5%86%B0%E5%86%B0&ie=utf-8&pn=" + str(x*50) for x in range(0, 10)]
    start_url = ["https://tieba.baidu.com/f?kw=%E7%8E%8B%E5%86%B0%E5%86%B0&ie=utf-8&pn=0"]
    #start_url = ["https://tieba.baidu.com/p/2962455717"]
    start_parser = [ArticleListParser]
    # start_proxies = proxy
    #start_COOKIES = cookie
    # start_in_process_filter = False
    # start_dupe_filter = False

class FetcherWorker2(nspider.FetcherWorker):
    def before_apply_request(self, request):
        # request.proxies = proxy
        return request

if __name__ == "__main__":
    settings = nspider.Settings()
    settings.recommend(tps=2)
    wbb_spider = WangBingbingSpider(settings=settings, fetcher_worker_class=FetcherWorker2)
    wbb_spider.start()

    # downloader = nspider.Downloader(tps=5, thread_num=5)
    # downloader.start(PageContentParser)