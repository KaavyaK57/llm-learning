import main
import json
from main import execute_query as exec


def generate_athena_query(message):


    model_id = "anthropic.claude-3-sonnet-20240229-v1:0"
    model_kwargs =  { 
        "max_tokens": 1024, "temperature": 0.1,
        "top_k": 50, "top_p": 0.1, "stop_sequences": ["\n\nHuman"],
    }
    
    conditions=f"""You are a assisting me in creating a athena query  .The database is {main.db} .
    There can be multiple tables in a given db.The table and column list are given in a nested python list {main.table_col_list}
      in ['table','column','description','data_type'] format
    Based on the column names understand user request and generate query.
    DO Remember:
        a.only give the query as result
        b.refere to the table and column list from {main.table_col_list} do not provide column names that are not present in a given table.
        c.All date columns are given as string and in 'MM/DD/YYYY' format and necessary conversion has to be added.
        d.Enclose column names in double string while giving query.Example "units sold"
    Based on the user input generate query
        """

    prompt = f"Human: \nThese are the conditions: {conditions}\nMain Text: {message}\nAssistant:"

    
    request_body = {
        "anthropic_version": "bedrock-2023-05-31",
        "max_tokens": 4096,  
        "temperature": 0,  
        "messages": [
            {
                "role": "user",
                "content": [{"type": "text", "text": prompt}],
            }
        ],
    }
    response = main.bedrock_runtime.invoke_model(modelId=model_id, body=json.dumps(request_body))
    result = json.loads(response["body"].read())
    generated_text = result.get("content", [])[0].get("text")
        
    return generated_text.strip()


message='which customer purchased the most number of products in a single day'
#query=generate_athena_query(message)
status=None
execution_id=None
#the below block is to generate query until it can be executed successfully
counter=0
while status!='SUCCEEDED' and counter<5:
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