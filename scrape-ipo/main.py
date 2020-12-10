import requests
from bs4 import BeautifulSoup
import sqlalchemy
import os

connection_name = os.getenv("INSTANCE_CONNECTION_NAME")
db_password = os.getenv("DATABASE_USER_PASSWORD")
db_name = "stock_production"
db_user = "root"
driver_name = 'mysql+pymysql'
query_string = dict({"unix_socket": "/cloudsql/{}".format(connection_name)})

def main(event, context):
    response = requests.get('https://www.jpx.co.jp/listing/stocks/new/')
    soup = BeautifulSoup(response.content, 'html.parser')
    rows = soup.select('tbody > tr')
    stock_count = int(len(rows) / 2)

    for i in range(0, stock_count):
        values = {}
        elms = rows[i * 2].select('td')

        dates = elms[0].text.split()
        values['ipo_date'] = dates[0]
        values['ipo_accepted_date'] = dates[1].strip('（）')
        values['name'] = elms[1].text.strip()
        values['stock_id'] = elms[2].text.strip()
        values['provisional_price'] = elms[5].text
        values['offering_volume'] = elms[6].text
        elms = rows[i * 2 + 1].select('td')
        values['market'] = elms[0].text
        values['offering_price'] = elms[3].text
        values['sale_volume'] = elms[4].text

        print(values)
        print(insert(values))


def insert(values):
    db = sqlalchemy.create_engine(
      sqlalchemy.engine.url.URL(
          drivername=driver_name,
          username=db_user,
          password=db_password,
          database=db_name,
          query=query_string,
      ),
      pool_size=5,
      max_overflow=2,
      pool_timeout=30,
      pool_recycle=1800
    )

    try:
        with db.connect() as conn:
            stmt1 = sqlalchemy.text("INSERT ignore INTO stocks (id, name) values (:id, :name);")
            conn.execute(stmt1, id=values['stock_id'], name=values['name'])

            stmt2 = sqlalchemy.text("INSERT INTO ipos (stock_id, ipo_date, offering_price) values (:stock_id, :ipo_date, :offering_price);")
            conn.execute(stmt2, stock_id=values['stock_id'], ipo_date=values['ipo_date'], offering_price=values['offering_price'])
    except Exception as e:
        return 'Error: {}'.format(str(e))
    return 'ok'


if __name__ == '__main__':
    main(None, None)
