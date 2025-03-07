from pathlib import Path

import geopandas as gpd


def main():
    data_folder = Path('data')

    network = gpd.read_file(
        data_folder / 'gis' / 'lastkajen' / 'Vägslag_516890.gpkg',
        columns=['EXTENT_LENGTH', 'geometry'])
    counties = gpd.read_file(
        data_folder / 'gis' / 'Topografi 1M Nedladdning, vektor' / 'administrativindelning_sverige.gpkg',
        layer='lansyta')

    case_area = counties.loc[counties['lansnamn'] == 'Östergötlands län']
    network = network[network.intersects(case_area['geometry'].union_all())]

    network['geometry'] = network['geometry'].force_2d()
    network = network.rename(columns={'EXTENT_LENGTH': 'cost'})

    network['reverse_cost'] = network['cost']

    network.to_file(data_folder / 'network.gpkg', driver='GPKG', index=False)


if __name__ == '__main__':
    main()
