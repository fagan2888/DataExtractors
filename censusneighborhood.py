import pg
import scrapecensus
import checkLocation

path = '/Users/tjhuang/Projects/TheDataExtractors/data/Neighborhood_Clusters/Neighborhood_Clusters'

if __name__ == '__main__':
    df = pg.saveTable('census_tract_coordinates')
    clusters = []
    neighborhoods = []

    for l, r in df.iterrows():
        cluster, neighborhood = checkLocation.getNeighborhoodClusterLatLon(
            path, float(r[1]), float(r[2]))
        clusters.append(cluster)
        neighborhoods.append(neighborhood)

    df['Cluster'] = clusters
    df['Neighborhood'] = neighborhoods
    df.columns = ['Census Tract', 'Latitude',
                  'Longitude', 'Cluster', 'Neighborhood']
    print(df)
    pg.createCensusTable(df)
