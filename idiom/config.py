# -*- coding: utf-8 -*-

catalog_list = list(range(98, 120)) + list(range(143, 147))
url_pattern = 'http://www.chinabaike.com/article/81/82/{0}/Article_{1}_{2}.html'
header_info = {
    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8',
    'Accept-Encoding': 'gzip, deflate',
    'Accept-Language': 'zh-CN,zh;q=0.9',
    'Cache-Control': 'max-age=0',
    'Connection': 'keep-alive',
    'Cookie': 'Hm_lvt_4f96c40d8eef1221b07b416aa5f28f6b=1540200994; __cfduid=d581a01392cd64ad849adba14f65a16081540201169; Hm_lpvt_4f96c40d8eef1221b07b416aa5f28f6b=1540201492',
    'Host': 'www.chinabaike.com',
    'If-Modified-Since': 'Wed, 12 Sep 2018 01:40:54 GMT',
    'Referer': 'http://www.chinabaike.com/article/81/82/145/Article_145_1.html',
    'Upgrade-Insecure-Requests': '1',
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/68.0.3440.75 Safari/537.36',
}

db_config = {
        'host': '172.18.88.116',
        'port': '3306',
        'user': 'user',
        'password': '88888888',
        'database': 'webfiles',
}