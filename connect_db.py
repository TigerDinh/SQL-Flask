import pyodbc


def connect_db():
    ODBC_STR = 'Driver={ODBC Driver 18 for SQL Server};Server=tcp:videostore.database.windows.net,1433;Database=VideoStore;Uid=Tiger;Pwd={iLikeToBake!};Encrypt=yes;TrustServerCertificate=no;Connection Timeout=30;'
    return pyodbc.connect(ODBC_STR)


if __name__ == '__main__':
    print(pyodbc.drivers())
    print(connect_db())
