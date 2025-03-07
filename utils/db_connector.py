import csv
from io import StringIO
from typing import Literal

import geopandas as gpd
import pandas as pd
from sqlalchemy import create_engine, text
from sqlalchemy.orm import DeclarativeBase


class DBC:
    """Database Connection Class for PostgreSQL Databases."""

    def __init__(self, host: str, database: str, port: str, user: str, password: str, driver: str = 'postgresql'):
        self.host = host
        self.database = database
        self.port = port
        self.user = user
        self.password = password
        self.driver = driver

    def _create_conn_url(self, driver: str = False) -> str:
        if driver:
            return f'{driver}://{self.user}:{self.password}@{self.host}/{self.database}'
        else:
            return f'{self.driver}://{self.user}:{self.password}@{self.host}/{self.database}'

    @staticmethod
    def psql_insert_copy(table, conn, keys, data_iter):
        """From: https://pandas.pydata.org/docs/user_guide/io.html#io-sql-method"""

        dbapi_conn = conn.connection  # gets a DBAPI connection that can provide a cursor
        with dbapi_conn.cursor() as cur:
            s_buf = StringIO()
            writer = csv.writer(s_buf)
            writer.writerows(data_iter)
            s_buf.seek(0)

            columns = ', '.join(['"{}"'.format(k) for k in keys])
            if table.schema:
                table_name = '{}.{}'.format(table.schema, table.name)
            else:
                table_name = table.name

            sql = 'COPY {} ({}) FROM STDIN WITH CSV'.format(
                table_name, columns)
            cur.copy_expert(sql=sql, file=s_buf)

    def create_tables(self, base: type[DeclarativeBase], replace: bool = False):
        engine = create_engine(self._create_conn_url())
        if replace:
            base.metadata.drop_all(engine)
        base.metadata.create_all(engine)

    def execute_query(self, query: str):
        engine = create_engine(self._create_conn_url())
        with engine.connect() as connection:
            with connection.begin():
                result = connection.execute(text(query))
                for row in result:
                    print(row)

    def query_2_df(self, query: str, **kwargs) -> pd.DataFrame:
        engine = create_engine(self._create_conn_url())
        return pd.read_sql_query(query, con=engine, **kwargs)

    def query_2_gdf(self, query: str, geom_col: str = "geometry", **kwargs) -> gpd.GeoDataFrame:
        engine = create_engine(self._create_conn_url())
        return gpd.GeoDataFrame.from_postgis(query, con=engine, geom_col=geom_col, **kwargs)

    def df_2_db(
            self,
            df: pd.DataFrame,
            table: str,
            schema: str = 'public',
            if_exists: Literal["fail", "replace", "append"] = 'append',
            index: bool = False,
            **kwargs
    ):
        engine = create_engine(self._create_conn_url())
        df.to_sql(
            con=engine,
            schema=schema,
            name=table,
            index=index,
            if_exists=if_exists,
            method=self.psql_insert_copy,
            **kwargs
        )

    def gdf_2_db(
            self,
            gdf: gpd.GeoDataFrame,
            table: str,
            schema: str = 'public',
            if_exists: Literal["fail", "replace", "append"] = 'append',
            index: bool = False,
            **kwargs
    ):
        engine = create_engine(self._create_conn_url())
        gdf.to_postgis(
            con=engine,
            schema=schema,
            name=table,
            index=index,
            if_exists=if_exists,
            **kwargs
        )
