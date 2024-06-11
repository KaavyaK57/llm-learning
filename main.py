import boto3
import pandas as pd
from io import StringIO
import json
# Initialize AWS Athena client
region='us-east-1'
athena_client = boto3.client('athena', region_name=region)
s3_client=boto3.client('s3',region_name=region)
bucket_name='kaavyak'
db='sales'

bedrock_runtime = boto3.client(
        service_name="bedrock-runtime",
        region_name="us-east-1",
    )
#model_id = "anthropic.claude-3-sonnet-20240229-v1:0"
"""model_kwargs =  { 
        "max_tokens": 1024, "temperature": 0.1,
        "top_k": 50, "top_p": 0.1, "stop_sequences": ["\n\nHuman"],
    }"""
def execute_query(query_string):
    # Execution parameters
    query_execution = athena_client.start_query_execution(
        QueryString=query_string,
        QueryExecutionContext={
            'Database': 'sales'
        },
        ResultConfiguration={
            'OutputLocation': 's3://kaavyak/athena_result/'
        }
    )
    
    # Get the query execution ID
    query_execution_id = query_execution['QueryExecutionId']
    
    # Check the status of the query execution
    status = None
    while status not in ['SUCCEEDED', 'FAILED', 'CANCELLED']:
        response = athena_client.get_query_execution(QueryExecutionId=query_execution_id)
        status = response['QueryExecution']['Status']['State']
        if status == 'SUCCEEDED':
            print("Query execution successful!")
            return query_execution_id,status
        elif status == 'FAILED' or status == 'CANCELLED':
            print("Query execution failed or cancelled.")
            return None,None
        else:
            pass
            #print("Query is still running...")
            # Optional: You can add a sleep here to avoid throttling
            # time.sleep(5)

def get_query_results(query_execution_id):
    # Get the results of the query execution
    response = athena_client.get_query_results(QueryExecutionId=query_execution_id)
    result = []
    #print(response)
    # Parse the result set
    
    for index, row in enumerate(response['ResultSet']['Rows']):
        if index == 0:
            # Skip the header row
            continue
        result.append([data.get('VarCharValue', '') for data in row['Data']])
    
    # Exclude header row
    #print('after processing',result)
    return result
def get_table_list(db):
    get_table_list_quey=f"""show tables in {db};"""
    execution_id,status=execute_query(get_table_list_quey)
    if execution_id:
        columns=get_query_results(execution_id)
        tables = [element for innerList in columns for element in innerList]
        return tables
    return None

def get_col_list(db,table_name):
    get_col_query=f""" SELECT 
                        table_name,column_name,data_type
                        FROM    
                        INFORMATION_SCHEMA.COLUMNS       
                        WHERE 
                    TABLE_NAME = '{table_name}'     
                    AND TABLE_SCHEMA = '{db}'; """
    execution_id,status=execute_query(get_col_query)
    if execution_id:
        columns=get_query_results(execution_id)
        col_list = [element for innerList in columns for element in innerList]
        return col_list
    return None

#message='which store has most of the products'
tables=get_table_list(db)
#print(tables)
for table in tables:
        if 'dict' in table.lower() :
            query=f"""select * from {table}"""
            execution_id,status = execute_query(query)
            table_col_list=get_query_results(execution_id)

#print(query)
#execution_id = execute_query(query)
#results=get_query_results(execution_id)
#print(query)
#print(results)

