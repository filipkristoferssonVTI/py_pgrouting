from pathlib import Path

import geopandas as gpd


def main():
    data_folder = Path('data')

    points = (gpd.read_file(
        data_folder / 'gis' / 'test_grid.gpkg',
        columns=['cellId', 'geometry'])
              .rename(columns={'cellId': 'id'}))

    points['geometry'] = points['geometry'].centroid

    points.to_file(data_folder / 'points.gpkg', driver='GPKG', index=False)


if __name__ == '__main__':
    main()
