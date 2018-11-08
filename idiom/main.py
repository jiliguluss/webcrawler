import requests
from bs4 import BeautifulSoup
from urllib.parse import urljoin
import re
import time
import random
import yaml
import logging
import os
import hashlib
import json
from logging import config as log_conf

from config import catalog_list, url_pattern, header_info
from parallel import MultiProcess


logger = logging.getLogger(__name__)
with open("logging.yaml", "r") as stream: log_conf.dictConfig(yaml.load(stream))


def extract_idiom_detail(url, header):
    html = requests.get(url=url, headers=header)
    soup = BeautifulSoup(html.content, 'html5lib')
    spell, comment, source = '', '', ''
    for idiom_tag in soup.select('table[bgcolor="#eeeecc"] tr') + soup.select('table[bgcolor="#EEEECC"] tr'):
        tag_name = idiom_tag.select('td')[0].text.strip()
        tag_text = idiom_tag.select('td')[1].text.strip()
        if tag_name.find('拼音') != -1:
            spell = re.sub('[\t\r\n]+', ' ', tag_text)
        elif tag_name.find('释义') != -1:
            comment = re.sub('[\t\r\n]+', ' ', tag_text)
        elif tag_name.find('出处') != -1:
            source = re.sub('[\t\r\n]+', ' ', tag_text)
    return spell, comment, source


def extract_idiom_table(page_list, header):
    tot_list = list()
    for page in page_list:
        idiom_list = list()
        html = requests.get(url=page, headers=header)
        soup = BeautifulSoup(html.content, 'html5lib')
        for idiom_tag in soup.select('table.tablist tr > td > a[title]'):
            time.sleep(2 + random.random())
            idiom = idiom_tag['title']
            url = urljoin(page, idiom_tag['href'])
            spell, comment, source = extract_idiom_detail(url, header)
            idiom_list.append([idiom, url, spell, comment, source])
            logger.info('成语：{1}'.format(url, idiom))
        tot_list.extend(idiom_list)
        list_to_json(idiom_list, page)
    return tot_list


def list_to_json(data_list, page_url):
    path = os.path.join(os.getcwd(), 'res')
    if not os.path.exists(path): os.makedirs(path)
    file_name = hashlib.md5(page_url.encode(encoding='utf-8')).hexdigest()
    with open(os.path.join(path, '{0}.json'.format(file_name)), 'w') as f:
        f.write(json.dumps(data_list, ensure_ascii=False))
        logging.info('页面：{0}\t数量：{1}'.format(page_url, len(data_list)))


def sub_proc(sole_dict, share_dict):
    urls = sole_dict.get('task_data')
    header = share_dict.get('header')
    sub_res = extract_idiom_table(urls, header)
    return sub_res


if __name__ == '__main__':
    catalog_list = [112]
    page_list = list()
    for catalog in catalog_list:
        url = url_pattern.format(catalog, str(catalog).zfill(3), 1)
        html = requests.get(url, header_info)
        soup = BeautifulSoup(html.content, 'html5lib')
        selector = soup.select('tr td[title="页次"]')
        if len(selector):
            page_num = int(re.match('(\d+)/(\d+)页', selector[0].text.strip()).group(2))
            page_list.extend([url_pattern.format(catalog, str(catalog).zfill(3), page_no) for page_no in range(1, page_num + 1)])
            selector = soup.select('tr td[title="总数"]')
            idiom_num = int(re.search('(\d+)', selector[0].text.strip()).group())
            logger.info('{0}类成语数量：{1}'.format(catalog, idiom_num))
    logger.info('待抓取页面数量：{0}'.format(len(page_list)))

    multi_proc = MultiProcess()
    param_dict = {'header': header_info, 'idiom_table': 'web_file_idiom'}
    res_list = multi_proc.main_proc(sub_proc, param_dict, len(page_list), page_list)



