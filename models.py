import os
import sqlalchemy
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import create_engine, Column, Integer, String, Date, DateTime, Boolean, ForeignKey, func, and_
from sqlalchemy.orm import sessionmaker, relationship, joinedload


Base = declarative_base()
session = None


def get_session():
    driver_name = 'mysql+pymysql'
    username = os.getenv('DB_USERNAME')
    database = os.getenv('DB_DATABASE')
    password = os.getenv('DB_PASSWORD')

    if "a":
        connection_str = sqlalchemy.engine.url.URL(
            drivername=driver_name,
            username=username,
            password=password,
            database=database,
            query={ "unix_socket": "/cloudsql/{}".format("a") }
        )
    else:
        connection_str = sqlalchemy.engine.url.URL(
            drivername=driver_name,
            host=os.getenv('DB_HOST'),
            username=username,
            password=password,
            database=database
        )

    print(connection_str)
    exit()
    global engine
    engine = create_engine(connection_str)
    session = sessionmaker(bind=engine)()
    return session


session = get_session()


class Stock(Base):
    __tablename__ = 'stocks'
    id = Column(Integer, primary_key=True)
    created_at = Column(DateTime, server_default=func.now())
    updated_at = Column(DateTime, server_default=func.now(), onupdate=func.now())
    name = Column(String(255))
    market = Column(String(255))
    ipo_date = Column(Date)
    ipo_accepted_date = Column(Date)
    offering_price = Column(Integer)
    provisional_price_min = Column(Integer)
    provisional_price_max = Column(Integer)
    offering_volume = Column(Integer)
    sale_volume = Column(Integer)
    file_1_url = Column(String(255))
    file_2_url = Column(String(255))
    file_3_url = Column(String(255))
    file_4_url = Column(String(255))
    file_5_url = Column(String(255))

    def create_or_update(values):
        stock = session.query(Stock).filter(Stock.id == values['id']).first()
        if not stock:
            stock = Stock()
            stock.id = values['id']

        stock.name = values['name']
        stock.market = values['market']
        stock.ipo_date = values['ipo_date']
        stock.ipo_accepted_date = values['ipo_accepted_date']
        stock.offering_price = values.get('offering_price')
        stock.provisional_price_min = values.get('provisional_price_min')
        stock.provisional_price_max = values.get('provisional_price_max')
        stock.offering_volume = values.get('offering_volume')
        stock.sale_volume = values.get('sale_volume')
        stock.file_1_url = values['file_1_url']
        stock.file_2_url = values['file_2_url']
        stock.file_3_url = values['file_3_url']
        stock.file_4_url = values.get('file_4_url')
        stock.file_5_url = values.get('file_5_url')

        session.add(stock)
        session.commit()


Base.metadata.create_all(engine)
