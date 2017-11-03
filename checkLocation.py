"""
Code written to convert Lat/Lon to neighborhood_cluster
"""

import shapefile
import geoid
from shapely.geometry import Point, shape


def getNeighborhoodClusterLatLon(path, lat, lon):
    # read your shapefile
    sf = shapefile.Reader(path)
    num_shapes = sf.numRecords

    # get the shapes
    shapes = sf.shapes()
    # format the point
    point = Point(lon, lat)

    # iterate through the shapes and check each polygon for the point
    for i in range(num_shapes):
        polygon = shape(shapes[i])
        if (polygon.contains(point)):
            cluster = sf.record(i)
            # return both the cluster neighborhood and neighborhood names
            return(cluster[2], cluster[3])

    return "Neighborhood not found."


if __name__ == '__main__':
    # Test code that loads shapefile and returns a neighborhood_cluster
    lat = 0
    lon = 0
    print("latitude is: " + str(lat))
    print("longitude is: " + str(lon))
    shp_file_base='Neighborhood_Clusters'
    dat_dir='/Users/anthonysanchez/Downloads/'+shp_file_base +'/'
    path = dat_dir+shp_file_base

    print("Neighborhood is: " + str(getNeighborhoodClusterLatLon(path,lat,lon)))
