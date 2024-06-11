import main
import json
from main import execute_query as exec

def generate_athena_query(message,query_list):
    
    model_id = "anthropic.claude-3-haiku-20240307-v1:0"
    model_kwargs =  { 
        "max_tokens": 1024, "temperature": 0.1,
        "top_k": 50, "top_p": 0.1, "stop_sequences": ["\n\nHuman"],
    }
    #print(main.table_col_list)
    system=f"""You are a assisting me in creating a athena query  .The database is {main.db} .
    There can be multiple tables in a given db.The table and column list are given in a nested python list 
    {main.table_col_list} in ['table','column','description','data_type'] format
    Based on the column names understand user request and generate query.
    DO Remember:
        a.only give the query as result
        b.refere to the table and column list from {main.table_col_list} do not provide column names that are not present in a given table.
        c.Use date_parse function and cast to DATE for date columns in string data type.The format is MM/DD/YYYY.Do not use substring as there might be some inconsistencies.
        d.To calculate difference between years use "year(date(column))-year(date(column))
        e.Enclose column names in double string while giving query.Example "units sold".
        f.{query_list} contains the list of queries for the same given text.
    Based on the user input generate query
        """
#c.All date columns are given as string and in 'MM/DD/YYYY' format .Convert it to YYYY-MM-DD format.Use date_parse function and then convert to cast when dates are given in string format.
    prompt =message
    body = {
        "anthropic_version": "bedrock-2023-05-31",
        "system": system,
        "messages": [
            {"role": "user", "content": [{"type": "text", "text": prompt}]},
        ],
    }
    body.update(model_kwargs)

    # Invoke
    response = main.bedrock_runtime.invoke_model(
        modelId=model_id,
        body=json.dumps(body),
    )

    # Process and print the response
    result = json.loads(response.get("body").read()).get("content", [])[0].get("text", "")
    return result    
 
#message='customers older than 40 years'

def check_generated_query(message):
    counter=0
    status=None
    execution_id=None
    query_list=[]
    while status!='SUCCEEDED' and counter<2:
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

message='which customer purchased the most number of products in a single day'
#query=generate_athena_query(message)
#query_list=[]
#print(query)
status=None
execution_id=None
check_generated_query(message)