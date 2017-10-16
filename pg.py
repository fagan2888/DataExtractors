from sqlalchemy import create_engine
import pandas as pd


def createEngine():
    engine = create_engine(
        'postgres://Jay:Huang@de-dbinstance.c6dfmakosb5f.us-east-1.rds.amazonaws.com:5432/dataextractorsDB')
    return engine


def readTable(nameTable, engine):
    stmt = 'SELECT * FROM ' + nameTable
    results = engine.execute(stmt)
    return results


def saveTable(nameTable=None):
    engine = createEngine()
    if nameTable == None:
        print(engine.table_names())
        nameTable = input("Enter table name: ")
    results = readTable(nameTable, engine)
    df = pd.DataFrame(results.fetchall())
    return df


def printTable(nameTable=None):
    engine = createEngine()
    if nameTable == None:
        print(engine.table_names())
        nameTable = input("Enter table name: ")
    results = readTable(nameTable, engine)
    for result in results:
        print(result)


def createCensusTable(df):
    engine = createEngine()
    print(engine.table_names())

    engine.execute("CREATE TABLE IF NOT EXISTS census_tracts \
                    (census text, latitude text, longitude text, cluster text, neighborhood text)")

    for lab, row in df.iterrows():
        engine.execute("INSERT INTO census_tracts (census, latitude, longitude, cluster, neighborhood) \
                        VALUES (%s, %s, %s, %s, %s)", row['Census Tract'], row['Latitude'], row['Longitude'], row['Cluster'], row['Neighborhood'])


def createCensusTableTest(df):
    engine = createEngine()
    print(engine.table_names())

    engine.execute("CREATE TABLE IF NOT EXISTS census_tracts_test1 \
                    (census text, latitude text, longitude text, cluster text, neighborhood text)")

    for lab, row in df.iterrows():
        engine.execute("INSERT INTO census_tracts_test1 (census, latitude, longitude, cluster, neighborhood) \
                        VALUES (?,?,?,?,?)", (row['Census Tract'], row['Latitude'], row['Longitude'], row['Cluster1'], row['Neighborhood1']))


def joinPopByCensusTract():
    engine = createEngine()
    stmt = "SELECT population.year, population.census, population.pop, census_tract_coordinates.latitude, census_tract_coordinates.longitude \
            FROM population \
            LEFT JOIN census_tract_coordinates on population.census = census_tract_coordinates.census"
    results = engine.execute(stmt)
    df = pd.DataFrame(results.fetchall())
    return df


def joinPovByCensusTract():
    engine = createEngine()
    stmt = "SELECT poverty.year, poverty.census, poverty.pov, census_tract_coordinates.latitude, census_tract_coordinates.longitude \
            FROM poverty \
            LEFT JOIN census_tract_coordinates on poverty.census = census_tract_coordinates.census"
    results = engine.execute(stmt)
    df = pd.DataFrame(results.fetchall())
    return df


def trimPopString():
    engine = createEngine()
    stmt = "UPDATE population \
            SET census=trim(trailing ', District of Columbia, District of Columbia' from census)"
    engine.execute(stmt)


def trimPovString():
    engine = createEngine()
    stmt = "UPDATE poverty \
            SET census=trim(trailing ', District of Columbia, District of Columbia' from census)"
    engine.execute(stmt)


if __name__ == '__main__':
    printTable('census_tracts')
