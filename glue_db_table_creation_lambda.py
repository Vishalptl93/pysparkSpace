import json
import boto3

def lambda_handler(event, context):
    glue = boto3.client('glue')
    #checking the Glue DB exists
    def checkGlueDb(dbName):
     try:
      responseGetDatabases  = glue.get_databases()
      databaseList = responseGetDatabases['DatabaseList']
      for databaseDict in databaseList:
          databaseName = databaseDict['Name']
          if databaseName==dbName:
              return True
      return False
     except Exception as e:
         print( e.__str__())
         return False
    
    #checking the Glue table exists under Glue DB
    def checkGlueTable(dbName,tableName):
        try:
         responseGetTables = glue.get_tables(DatabaseName=dbName)
         tableList = responseGetTables['TableList']
         for table in tableList:
            if table.get('Name')==tableName:
                return True
         return False
        except Exception as e:
            print(e.__str__())
            return False
    
    
    
    if(checkGlueDb('testdb')==False):
     db = glue.create_database(
    
       DatabaseInput = {'Name': 'testdb'}
    
     )
     print("your database is created")
    
    if(checkGlueTable('testdb','employees')==False):
     tbl = glue.create_table(
    
       DatabaseName = 'testdb',
    
       TableInput = {
    
          'Name': 'employees',
    
          'StorageDescriptor': {
    
             'Columns': [
    
                 {
                     "Name": "name",
                     "Type": "string",
                     "Comment": ""
                 },
                 {
                     "Name": "department",
                     "Type": "string",
                     "Comment": ""
                 },
                 {
                     "Name": "manager",
                     "Type": "string",
                     "Comment": ""
                 },
                 {
                     "Name": "salary",
                     "Type": "string",
                     "Comment": ""
                 }
    
             ],
            'Location': "s3://vishaldl/emp",
            'InputFormat': 'org.apache.hadoop.mapred.TextInputFormat',
            'OutputFormat': 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat',
            'Compressed': False,
            'SerdeInfo': {  'SerializationLibrary': 'org.apache.hadoop.hive.serde2.OpenCSVSerde'}
    
    
          },
          'TableType' : "EXTERNAL_TABLE"
    
       }
    
     )
    
    # TODO implement
    return {
        'statusCode': 200,
        'body': json.dumps('Hello from Lambda!')
    }
