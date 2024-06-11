import main
from main import execute_query as exec
import json
import pandas as pd

def generate_athena_query(message,query_list):
    prompt = f"""This is a task converting text into a Athena query.The database is {main.db} .
    There can be multiple tables in a given db.The table and column list are given in a nested python list {main.table_col_list} 
    in ['table','column','description','data_type'] format
    Based on the column names understand user request and generate query .
    Do remember
        a.All date columns are given as string and in 'month/date/year' format and necessary conversion has to be added
        b.Enclose column names in double string while giving query.Example "units sold.
        c.{query_list} contains the list of queries that has failed for the given text and different query has to be created next time.Do not give the same query again.
    Here is the test question to be answered: Convert text to SQL: [Q]{message}.
"""
    body = json.dumps({
    "inputText": prompt, 
    "textGenerationConfig":{  
        "maxTokenCount":256,
        "stopSequences":[], #define phrases that signal the model to conclude text generation.
        "temperature":0.1, #Temperature controls randomness; higher values increase diversity, lower values boost predictability.
        "topP":0.9 # Top P is a text generation technique, sampling from the most probable tokens in a distribution.
    }
    })

    response = main.bedrock_runtime.invoke_model(
    body=body,
	modelId="amazon.titan-text-lite-v1",
    #"amazon.titan-text-express-v1",
    accept="application/json", 
    contentType="application/json"
    )   
    response_body = json.loads(response.get('body').read())
    outputText = response_body.get('results')[0].get('outputText')
    return outputText



#the below block is to generate query until it can be executed successfully
def check_generated_query(message):
    counter=0
    status=None
    execution_id=None
    query_list=[]
    while status!='SUCCEEDED' and counter<5:
        query=generate_athena_query(message,query_list)
        if 'sql' in query:
            c='`'
            query_position=[pos for pos, char in enumerate(query) if char == c]
            len_n=query_position[len(query_position)-1]
            query=query[query_position[0]:len_n].replace('sql',"").replace('`',"")
            #print(query)
            #status='SUCCEEDED'
        query_list.append(query)
        print(query)
        execution_id,status=exec(query)
        counter+=1

message='which category product was mostly sold among female customers'
#query=generate_athena_query(message)
#query_list=[]
#print(query)
status=None
execution_id=None
check_generated_query(message)