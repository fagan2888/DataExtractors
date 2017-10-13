import pandas as pd
from sqlalchemy import create_engine, MetaData, Table, Column, String, Integer, Boolean, insert

def createPopulationTable():
    population = Table('population', metadata,
        Column('year', String()),
        Column('id', String()),
        Column('census', String()),
        Column('pop', Integer()))
    population.create()

    pop2011 = pd.read_csv('population_2011.csv', header=1, usecols=['Id', 'Geography', 'Estimate; Total:'])
    pop2011.rename(columns={'Id':'ID', 'Geography':'Census Tract', 'Estimate; Total:':'Population'}, inplace=True)
    for lab,row in pop2011.iterrows():
        stmt = insert(population).values(year='2011', id=row['ID'], census=row['Census Tract'], pop=row['Population'])
        result = connection.execute(stmt)

    pop2012 = pd.read_csv('population_2012.csv', header=1, usecols=['Id', 'Geography', 'Estimate; Total:'])
    pop2012.rename(columns={'Id':'ID', 'Geography':'Census Tract', 'Estimate; Total:':'Population'}, inplace=True)
    for lab,row in pop2012.iterrows():
        stmt = insert(population).values(year='2012', id=row['ID'], census=row['Census Tract'], pop=row['Population'])
        result = connection.execute(stmt)

    pop2013 = pd.read_csv('population_2013.csv', header=1, usecols=['Id', 'Geography', 'Estimate; Total:'])
    pop2013.rename(columns={'Id':'ID', 'Geography':'Census Tract', 'Estimate; Total:':'Population'}, inplace=True)
    for lab,row in pop2013.iterrows():
        stmt = insert(population).values(year='2013', id=row['ID'], census=row['Census Tract'], pop=row['Population'])
        result = connection.execute(stmt)

    pop2014 = pd.read_csv('population_2014.csv', header=1, usecols=['Id', 'Geography', 'Estimate; Total:'])
    pop2014.rename(columns={'Id':'ID', 'Geography':'Census Tract', 'Estimate; Total:':'Population'}, inplace=True)
    for lab,row in pop2014.iterrows():
        stmt = insert(population).values(year='2014', id=row['ID'], census=row['Census Tract'], pop=row['Population'])
        result = connection.execute(stmt)

    pop2015 = pd.read_csv('population_2015.csv', header=1, usecols=['Id', 'Geography', 'Estimate; Total:'])
    pop2015.rename(columns={'Id':'ID', 'Geography':'Census Tract', 'Estimate; Total:':'Population'}, inplace=True)
    for lab,row in pop2015.iterrows():
        stmt = insert(population).values(year='2015', id=row['ID'], census=row['Census Tract'], pop=row['Population'])
        result = connection.execute(stmt)

def createPovertyTable():
    poverty = Table('poverty', metadata,
        Column('year', String()),
        Column('id', String()),
        Column('census', String()),
        Column('pov', Integer()))
    poverty.create()

    pov2012 = pd.read_csv('poverty_2012.csv', header=1, usecols=['Id', 'Geography', 'Below poverty level; Estimate; Population for whom poverty status is determined'])
    pov2012.rename(columns={'Id':'ID', 'Geography':'Census Tract', 'Below poverty level; Estimate; Population for whom poverty status is determined':'Poverty'}, inplace=True)
    for lab,row in pov2012.iterrows():
        stmt = insert(poverty).values(year='2012', id=row['ID'], census=row['Census Tract'], pov=row['Poverty'])
        result = connection.execute(stmt)

    pov2013 = pd.read_csv('poverty_2013.csv', header=1, usecols=['Id', 'Geography', 'Below poverty level; Estimate; Population for whom poverty status is determined'])
    pov2013.rename(columns={'Id':'ID', 'Geography':'Census Tract', 'Below poverty level; Estimate; Population for whom poverty status is determined':'Poverty'}, inplace=True)
    for lab,row in pov2013.iterrows():
        stmt = insert(poverty).values(year='2013', id=row['ID'], census=row['Census Tract'], pov=row['Poverty'])
        result = connection.execute(stmt)

    pov2014 = pd.read_csv('poverty_2014.csv', header=1, usecols=['Id', 'Geography', 'Below poverty level; Estimate; Population for whom poverty status is determined'])
    pov2014.rename(columns={'Id':'ID', 'Geography':'Census Tract', 'Below poverty level; Estimate; Population for whom poverty status is determined':'Poverty'}, inplace=True)
    for lab,row in pov2014.iterrows():
        stmt = insert(poverty).values(year='2014', id=row['ID'], census=row['Census Tract'], pov=row['Poverty'])
        result = connection.execute(stmt)

    pov2015 = pd.read_csv('poverty_2015.csv', header=1, usecols=['Id', 'Geography', 'Below poverty level; Estimate; Population for whom poverty status is determined'])
    pov2015.rename(columns={'Id':'ID', 'Geography':'Census Tract', 'Below poverty level; Estimate; Population for whom poverty status is determined':'Poverty'}, inplace=True)
    for lab,row in pov2015.iterrows():
        stmt = insert(poverty).values(year='2015', id=row['ID'], census=row['Census Tract'], pov=row['Poverty'])
        result = connection.execute(stmt)

if __name__ == '__main__':
    engine = create_engine('postgres://Jay:Huang@de-dbinstance.c6dfmakosb5f.us-east-1.rds.amazonaws.com:5432/dataextractorsDB')
    connection = engine.connect()
    metadata = MetaData(engine)

    print(engine.table_names())
