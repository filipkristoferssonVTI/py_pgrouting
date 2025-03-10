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


class PointTable(Base):
    __tablename__ = 'points'
    __table_args__ = {"schema": "public"}

    id = Column(Integer, primary_key=True)
    geometry = Column(Geometry('POINT', srid=3006))


class ODMatrixTable(Base):
    __tablename__ = 'od_matrix'
    __table_args__ = {"schema": "public"}

    id = Column(Integer, primary_key=True, autoincrement=True)
    origin = Column(Integer, nullable=False)
    destination = Column(Integer, nullable=False)
    cost = Column(Float)


class Queries:
    CREATE_TOPOLOGY = """
        SELECT pgr_createTopology('roads', 0.0001, 'geometry', clean := true);
    """

    ASSIGN_NEAREST_NODE = """
        ALTER TABLE points ADD COLUMN IF NOT EXISTS nearest_node INTEGER;
        UPDATE points
        SET nearest_node = (
            SELECT id
            FROM roads_vertices_pgr
            ORDER BY the_geom <-> points.geometry
            LIMIT 1
        );
    """

    @staticmethod
    def COMPUTE_OD_MATRIX(origin_nodes):
        origins_str = ", ".join(map(str, origin_nodes))
        return f"""
            WITH cost_matrix AS (
                SELECT * FROM pgr_dijkstraCost(
                    'SELECT id, source, target, cost FROM roads',
                    (SELECT array_agg(nearest_node) FROM points WHERE id IN ({origins_str})),
                    (SELECT array_agg(nearest_node) FROM points),
                    directed := false
                )
            )
            INSERT INTO od_matrix (origin, destination, cost)
            SELECT 
                p1.id AS origin,
                p2.id AS destination,  
                c.agg_cost
            FROM cost_matrix c
            JOIN points p1 ON c.start_vid = p1.nearest_node
            JOIN points p2 ON c.end_vid = p2.nearest_node;
        """


def main():
    data_folder = Path('data')
    network = gpd.read_file(data_folder / 'network.gpkg')
    points = gpd.read_file(data_folder / 'points.gpkg')

    load_dotenv()

    dbc = DBC(user=os.getenv("POSTGRES_USER"),
              password=os.getenv("POSTGRES_PASSWORD"),
              database=os.getenv("POSTGRES_DB"),
              host=os.getenv("POSTGRES_HOST"),
              port=os.getenv("POSTGRES_PORT"))

    dbc.create_tables(Base, replace=True)

    dbc.gdf_2_db(network, table='roads')
    dbc.gdf_2_db(points, table='points')

    dbc.execute_query(Queries.CREATE_TOPOLOGY)
    dbc.execute_query(Queries.ASSIGN_NEAREST_NODE)
    dbc.execute_query(Queries.COMPUTE_OD_MATRIX([30, 40, 50]))

    OD = dbc.query_2_df("SELECT * FROM od_matrix")

    DEBUG = 1


if __name__ == '__main__':
    main()
