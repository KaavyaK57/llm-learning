import main
from main import execute_query as exec
import json
import pandas as pd

def generate_athena_query(message,query_list):
    #print(main.table_col_list)

    prompt  = f"""
    Human: There is a database {main.db} with multiple tables .
    This python dictionary {main.table_col_dict} contains the table and column names in 'table':[columns]' format
    The description and datatype can be found in a nested python list {main.table_col_list} 
    in ['table','column','description','data_type'] format.
    All date columns are in string in MM/DD/YYY and can be inconsistent and conversion should be done.
    Enclose column names in double quotes.Example "Units Sold"
    {query_list} contains queries that are failed at execution for the same request and the same query should not be repeated.
    Just give query as response and nothing else and meaningfull name for derived columns.Can you generate Athena query for the below .: 
    {message}

    Assistant:
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
	modelId="amazon.titan-text-premier-v1:0",
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
    while status!='SUCCEEDED' and counter<2:
        query=generate_athena_query(message,query_list)
        if 'sql' in query:
            c='"'
            query_position=[pos for pos, char in enumerate(query) if char == c]
            len_n=query_position[len(query_position)-1]
            query=query[query_position[0]:len_n].replace('sql',"").replace('`',"")
            #print(query)
            #status='SUCCEEDED'
        query_list.append(query)
        print(query)
        execution_id,status=exec(query)
        counter+=1

message='which customer has purchased the most number of products in a single day'
#query=generate_athena_query(message)
#query_list=[]
#print(query)
status=None
execution_id=None
check_generated_query(message)