import requests
from bs4 import BeautifulSoup

def main(event, context):
    response = requests.get('https://www.jpx.co.jp/listing/stocks/new/')
    soup = BeautifulSoup(response.content, 'html.parser')
    rows = soup.select('tbody > tr')
    row_count = len(rows)

    i = 0
    while i < row_count:
        values = {}
        elms = rows[i].select('td')

        dates = elms[0].text.split()
        values['ipo_date'] = dates[0]
        values['ipo_accepted_date'] = dates[1].strip('()')
        values['name'] = elms[1].text
        values['code'] = elms[2].text
        values['provisional_price'] = elms[5].text
        values['offering_volume'] = elms[6].text

        i += 1
        elms = rows[i].select('td')
        values['market'] = elms[0].text
        values['offering_price'] = elms[3].text
        values['sale_volume'] = elms[4].text

        print(values)

        i += 1
