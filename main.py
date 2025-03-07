import os
from pathlib import Path

import geopandas as gpd
from dotenv import load_dotenv
from geoalchemy2 import Geometry
from sqlalchemy import Integer, Column, Float
from sqlalchemy.orm import DeclarativeBase

from utils.db_connector import DBC


class Base(DeclarativeBase):
    pass


class RoadTable(Base):
    __tablename__ = 'roads'
    __table_args__ = {"schema": "public"}

    id = Column(Integer, primary_key=True, autoincrement=True)
    source = Column(Integer)
    target = Column(Integer)
    cost = Column(Float, nullable=False)
    reverse_cost = Column(Float, nullable=False)
    geometry = Column(Geometry('LINESTRING', srid=3006))


def main():
    data_folder = Path('data')
    network = gpd.read_file(data_folder / 'network.gpkg')

    load_dotenv()

    dbc = DBC(user=os.getenv("POSTGRES_USER"),
              password=os.getenv("POSTGRES_PASSWORD"),
              database=os.getenv("POSTGRES_DB"),
              host=os.getenv("POSTGRES_HOST"),
              port=os.getenv("POSTGRES_PORT"))

    dbc.create_tables(Base, replace=True)
    dbc.gdf_2_db(network, table='roads')

    dbc.execute_query("SELECT pgr_createTopology('roads', 0.0001, 'geometry', clean := true);")


if __name__ == '__main__':
    main()
