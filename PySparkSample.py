
from hdbcli import dbapi

# Step 1: Set connection parameters
host = '192.168.2.188'      # e.g., 'hana.mycompany.com'
port = 30015                 # default port
user = 'UTL0555'              # or your DB user
password = 'Utl@0555'   # your password

try:
    # Step 2: Connect to SAP HANA
    connection = dbapi.connect(
        address=host,
        port=port,
        user=user,
        password=password
    )
    
    print("Connection to SAP HANA successful!")

    # Step 3: Run a simple query
    cursor = connection.cursor()    
    cursor.execute('SELECT "ItemCode", "ItemName", "ItmsGrpCod","OnHand",TO_VARCHAR(CURRENT_TIMESTAMP, \'YYYY-MM-DD hh:mi:ss:ff3AM\') AS "CurrentDate" FROM "INT_WMSDEVSAPHANA"."OITM"')
    #cursor.execute('SELECT Top 100 "TransNum", "CardCode", "CardName","Warehouse","CreateDate","InQty","OutQty" FROM "INT_WMSDEVSAPHANA"."OINM"')
    #cursor.execute('SELECT count(*) FROM "INT_WMSDEVSAPHANA"."OINM"')
    #cursor.execute('SELECT "ItmsGrpCod", "ItmsGrpNam" FROM "INT_WMSDEVSAPHANA"."OITB"')
    #cursor.execute('call "INT_WMSDEVSAPHANA"."GET_SUGGESTED_BIN_LIST"(''WMS00019'')')
    print("Query Results:")
    for row in cursor.fetchall():
        print(row)

    # Step 4: Cleanup
    cursor.close()
    connection.close()

except Exception as e:
    print("Connection failed:", str(e))
