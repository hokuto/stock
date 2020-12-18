import requests
from bs4 import BeautifulSoup
import sqlalchemy
import os

app_env = os.getenv("APP_ENV")
connection_name = os.getenv("INSTANCE_CONNECTION_NAME")
db_password = os.getenv("DATABASE_USER_PASSWORD")
db_name = "stock_production"
db_user = "root"
driver_name = 'mysql+pymysql'
query_string = dict({"unix_socket": "/cloudsql/{}".format(connection_name)})


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

            stmt2 = sqlalchemy.text("INSERT INTO ipos (stock_id, ipo_date) values (:stock_id, :ipo_date);")
            conn.execute(stmt2, stock_id=values['stock_id'], ipo_date=values['ipo_date'])
    except Exception as e:
        return 'Error: {}'.format(str(e))
    return 'ok'
