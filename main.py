import os, re
from urllib.parse import urljoin
import requests
from bs4 import BeautifulSoup
import sqlalchemy

from models import Stock


connection_name = os.getenv("INSTANCE_CONNECTION_NAME")
db_password = os.getenv("DATABASE_USER_PASSWORD")
db_name = "stock_production"
db_user = "root"
driver_name = 'mysql+pymysql'
query_string = dict({"unix_socket": "/cloudsql/{}".format(connection_name)})

def main(event, context):
    base_url = 'https://www.jpx.co.jp'
    target_urls = [urljoin(base_url, '/listing/stocks/new/')]

    for target_url in target_urls:
        response = requests.get(target_url)
        soup = BeautifulSoup(response.content, 'html.parser')
        rows = soup.select('tbody > tr')
        stock_count = int(len(rows) / 2)

        for i in range(0, stock_count):
            values = {}

            elms = rows[i * 2].select('td')
            dates = elms[0].text.split()
            values['ipo_date'] = dates[0]
            values['ipo_accepted_date'] = dates[1].strip('（）')
            values['name'] = elms[1].text.strip().split("\n")[0]
            values['id'] = elms[2].text.strip()
            values['file_1_url'] = urljoin(base_url, elms[3].find('a').get('href'))
            values['file_2_url'] = urljoin(base_url, elms[4].find('a').get('href'))
            provisional_price_range = elms[5].text
            if re.search('～', provisional_price_range):
                provisional_price_list = provisional_price_range.split('～')
                values['provisional_price_min'] = normalize_number_str(provisional_price_list[0])
                values['provisional_price_max'] = normalize_number_str(provisional_price_list[1])
            offering_volume_k = normalize_number_str(elms[6].text)
            if offering_volume_k:
                values['offering_volume'] = offering_volume_k * 1000

            elms = rows[i * 2 + 1].select('td')
            values['market'] = elms[0].text
            values['file_3_url'] = urljoin(base_url, elms[1].find('a').get('href'))
            link = elms[2].find('a')
            if link:
                values['file_4_url'] = urljoin(base_url, link.get('href'))
            values['offering_price'] = normalize_number_str(elms[3].text)
            sale_volume_k = normalize_number_str(elms[4].text)
            if sale_volume_k:
                values['sale_volume'] = sale_volume_k * 1000
            link = elms[5].find('a')
            if link:
                values['file_5_url'] = urljoin(base_url, link.get('href'))
            Stock.create_or_update(values)


def normalize_number_str(str):
    str = str.replace(',', '')
    m = re.match('[\d.]+', str)
    if m:
        return float(m[0])


if __name__ == '__main__':
    main(None, None)
