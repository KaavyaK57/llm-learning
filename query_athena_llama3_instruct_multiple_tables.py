import main
from main import execute_query as exec
import json
import pandas as pd

def generate_athena_query(message):

    prompt=f"""<s>[INST]Generate Athena query for {message} based on the given python list {main.table_col_list} 
    in '['table','column','description']' format.
    Remember:
    a.Just return the query no expalination needed.
    b.All dates are given in string MM/DD/YYYY format
    [/INST]"""

    model_id = "meta.llama3-70b-instruct-v1:0"
    #prompt = f"Human: \nThese are the conditions: {conditions}\nMain Text: {message}\nAssistant:"    
    body = json.dumps({
            "temperature": 0.0,
            "max_gen_len": 512,
            "prompt": prompt,
            "stop":["</s>"]

        })
    response = main.bedrock_runtime.invoke_model(
    body = body,
    modelId = model_id
    )
    print(response)
    response_body = json.loads(response.get("body").read())
    df = pd.DataFrame(
    {"Response": [response_body['outputs'][0]['text'].replace("\n", " ")]}
    )
    return response_body['outputs'][0]['text']



message='which customer purchased the most number of products in a single day'
#query=generate_athena_query(message)
#print(query)
status=None
execution_id=None
#the below block is to generate query until it can be executed successfully
counter=0
while status!='SUCCEEDED' and counter<2:
    query=generate_athena_query(message)
    if 'sql' in query:
        c='`'
        query_position=[pos for pos, char in enumerate(query) if char == c]
        len_n=query_position[len(query_position)-1]
        query=query[query_position[0]:len_n].replace('sql',"").replace('`',"")
        #print(query)
        #status='SUCCEEDED'
    print(query)
    execution_id,status=exec(query)
    counter+=1