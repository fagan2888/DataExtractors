from sqlalchemy import create_engine

def readTable(nameTable):
    stmt = 'SELECT * FROM ' + nameTable
    results = engine.execute(stmt)
    for result in results:
        print(result)

def trimPopString():
    engine.execute("UPDATE population SET census=trim(trailing ', District of Columbia, District of Columbia' from census)")

def trimPovString():
    engine.execute("UPDATE poverty SET census=trim(trailing ', District of Columbia, District of Columbia' from census)")

if __name__ == '__main__':
    engine = create_engine('postgres://Jay:Huang@de-dbinstance.c6dfmakosb5f.us-east-1.rds.amazonaws.com:5432/dataextractorsDB')
    print(engine.table_names())

    nameTable = input("Enter table name: ")
    readTable(nameTable)
