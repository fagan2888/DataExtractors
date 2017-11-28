"""
Tony Sanchez
11/23/2017
Georgetown Cohort 10: The Data Extractors Team

Description:
This is Python code generates all of my statistics per monthly
and per year for each neighborhood_cluster.
import computationAnalysis as ca
"""

import pandas as pd
import numpy as np
import loadCrimeIncidents as lc

def getStatsNeighborhood(df, nhDF, frm, to):
    #Get date range
    #drange = lc.getDateRangeDF(frm, to)
    #Get the basic time differences
    yd, md, fy, ty, fm, tm = getDateDiff(frm, to)
    firstyr_str = retEndFYStr(fy, fm)
    lastyr_str = retBegLYStr(ty, tm)

    #Set up return DF with added columns for mean, med and perc diff
    nhDF['n_mean'] = nhDF.apply(lambda _: '', axis=1)
    nhDF['n_med'] = nhDF.apply(lambda _: '', axis=1)
    nhDF['fy_mean'] = nhDF.apply(lambda _: '', axis=1)
    nhDF['ly_mean'] = nhDF.apply(lambda _: '', axis=1)
    nhDF['perc_growth'] = nhDF.apply(lambda _: '', axis=1)
    nhDF.columns = ['n_cluster','mean','med','fy_mean','ly_mean','perc_growth']

    for index, row in nhDF.iterrows():
        #for each row, extract each neighborhood_cluster
        ncDF = df[df['n_cluster'] == nhDF.n_cluster[index]]
        #calc mean and med for the entire timeline
        nhDF['mean'][index] = ncDF.num_crimes.mean()
        nhDF['med'][index] = ncDF.num_crimes.median()
        #get mean of first and last years to compare
        first_yearDF = getYearlyData(ncDF, frm, firstyr_str)
        last_yearDF = getYearlyData(ncDF, lastyr_str, to)
        nhDF['fy_mean'][index] = first_yearDF.num_crimes.mean()
        nhDF['ly_mean'][index] = last_yearDF.num_crimes.mean()
        # Calculate percent difference between first and last years
        nhDF['perc_growth'][index] = getPercDiff(first_yearDF.num_crimes.mean(), \
            last_yearDF.num_crimes.mean())

    return nhDF

def getStatsWashDC(df, frm, to):
    #Get the basic time differences
    yd, md, fy, ty, fm, tm = getDateDiff(frm, to)
    # Calc total city wide mean
    total_citycrime_mean = df.num_crimes.mean()
    """ Calculating Growth from first and last year of date range """
    #mean of first year
    firstyr_str = retEndFYStr(fy, fm)
    first_yearDF = getYearlyData(df, frm, firstyr_str)
    #mean of last year
    lastyr_str = retBegLYStr(ty, tm)
    last_yearDF = getYearlyData(df, lastyr_str, to)
    #Caclulate Growth over the time period
    growth = getPercDiff(first_yearDF.num_crimes.mean(), \
        last_yearDF.num_crimes.mean())

    return(total_citycrime_mean, growth)

#Returns a string with the end of the first year for stats
def retEndFYStr(fy, fm):
    fy = fy + 1
    if fm > 9:
        first_year_to = str(fy) + "-" + str(fm)
    else:
        first_year_to = str(fy) + "-0" + str(fm)
    return first_year_to

#Returns a string with the beginning of the last year for stats
def retBegLYStr(ty, tm):
    ty = ty - 1
    if tm > 9:
        first_year_frm = str(ty) + "-" + str(tm)
    else:
        first_year_frm = str(ty) + "-0" + str(tm)
    return first_year_frm

def getPercDiff(start, end):
    if end == 0:
        return 0
    else:
        growth = ((end - start) / end) * 100
        return growth

def getCityStatsDC(df):
    citydf = df.describe()
    return citydf

def getYearlyData(df, frm, to):
    yrDF = df[(df['year_month'] >= frm) & (df['year_month'] < to)]
    return yrDF

def getMonthlyValues(df, month):
    dfMnt = df[df['year_month'] == month]

def getNeighborhoodValues(df, nbh):
    dfNbh = df[df['n_cluster'] == nbh]

def getNeighorhoodClusterDF(path):
    engine = create_engine(path)
    nhClusterQuery = query2 = 'SELECT "Name" FROM neighborhood_clusters'
    df2 = pd.read_sql_query(nhClusterQuery, engine)
    #Reorder Neighborhood DF
    df2 = df2.sort_values(by='Name')
    df2 = df2.reset_index(drop=True)
    return(df2)

#Params that may be needed later.
def getDateDiff(frm, to):
    fy = int(frm[:4])
    ty = int(to[:4])
    fm = int(frm[5:])
    tm = int(to[5:])
    yearsdiff = (ty-fy)
    monthsdiff = (tm-fm)
    return (yearsdiff, monthsdiff, fy, ty, fm, tm)

def addNeighborhoodScore(df, citywide_growth):
    df['n_score'] = df.apply(lambda _: '', axis=1)

    for i in range(len(df)):
        #Adjusting growth to reflect distance from citywide_growth
        growth = df.perc_growth[i] - citywide_growth
        if (growth >= -10 and growth <= 10.99):
            df.n_score[i] = 0
        elif (growth >= -11 and growth <= 20.99):
            df.n_score[i] = -1
        elif (growth >= -21 and growth <= 30.99):
            df.n_score[i] = -2
        elif (growth >= -31 and growth <= 40.99):
            df.n_score[i] = -3
        elif (growth >= 41):
            df.n_score[i] = -4
        elif (growth >= -20.99 and growth <= -10.01):
            df.n_score[i] = 1
        elif (growth >= -30.99 and growth <= -21):
            df.n_score[i] = 2
        elif (growth >= -40.99 and growth <= -31):
            df.n_score[i] = 3
        elif (growth <= -41):
            df.n_score[i] = 4
        else:
            cluster = df.n_cluster[i]
            print(cluster + " does not fit!")
            df.n_score[i] = 0
    return(df)


if __name__ == '__main__':
    print("Starting Stats Gen...")
    path = 'postgres://Tony:Sanchez@de-dbinstance.c6dfmakosb5f.us-east-1.rds.amazonaws.com:5432/dataextractorsDB'
    frm = '2010-01'
    to = '2017-01'
    """" Three calls to get final processed Crime DataFrame """
    #returns full date range expected
    drDF = lc.getDateRangeDF(frm, to)
    #returns full crime_incidents DF with n_cluster DF
    baseDF, nhDF = lc.getBaseDF(path, frm, to)
    #pass in both DFs to get final edited DF
    df = lc.insertEmptyRows(baseDF, nhDF, drDF)

    total_citycrime_mean, citywide_growth = getStatsWashDC(df, frm, to)

    print("DC overall crime mean: " + str(total_citycrime_mean))
    print("DC growth over period: " + str(citywide_growth))
    new_nhdf = getStatsNeighborhood(df, nhDF, frm, to)

    scoresDF = addNeighborhoodScore(new_nhdf, citywide_growth)
    print(scoresDF)
