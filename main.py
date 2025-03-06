import os

import geopandas as gpd
from dotenv import load_dotenv
from shapely import Point

from utils.db_connector import DBC


def main():
    load_dotenv()

    dbc = DBC(user=os.getenv("POSTGRES_USER"),
              password=os.getenv("POSTGRES_PASSWORD"),
              database=os.getenv("POSTGRES_DB"),
              host=os.getenv("POSTGRES_HOST"),
              port=os.getenv("POSTGRES_PORT"))

    data = {
        'Name': ['Alice', 'Bob', 'Charlie'],
        'Age': [25, 30, 35],
        'geometry': [Point(-74.006, 40.7128), Point(-118.2437, 34.0522), Point(-87.6298, 41.8781)]
    }

    gdf = gpd.GeoDataFrame(data, geometry='geometry', crs='EPSG:3006')

    dbc.gdf_2_db(gdf=gdf, table='test', if_exists='replace')

    test = dbc.query_2_gdf("""SELECT * FROM test""")

    DEBUG = 1


if __name__ == '__main__':
    main()
