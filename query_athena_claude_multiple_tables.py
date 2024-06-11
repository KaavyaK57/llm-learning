import main
import json

def generate_athena_query(message):
    
    model_id = "anthropic.claude-v2"
	
            
        

    system=f"""You are a assisting me in creating a athena query  .The database is {main.db} .
    There can be multiple tables in a given db.The table and column list are given in a nested python list {main.table_col_list} in ['table','column','description'] format
    Based on the column names understand user request and generate query.
    DO Remember:
        a.only give the query as result
        b.refere to the table and column list from {main.table_col_list} do not provide column names that are not present in a given table.
        c.All date columns are given as string and in 'MM/DD/YYYY' format and necessary conversion has to be added
        c.Enclose column names in double string while giving query.Example "units sold"
    Based on the user input generate query
        """
    #prompt =message
    body = {
        "prompt": f"\n\nHuman:{message}\n\nAssistant:{system}",
        "max_tokens_to_sample": 1024,
        "temperature": 0.1,
        "top_p": 0.9,}
    #body.update(model_kwargs)

    # Invoke
    response = main.bedrock_runtime.invoke_model(
        modelId=model_id,
        body=json.dumps(body),
    )

    # Process and print the response
    #print(response)
    #result = json.loads(response.get("body").read()).get("content", [])[0].get("text", "")
    response_body = json.loads(response.get('body').read())
    
    return response_body.get('completion')
    
 
message='which category product was mostly sold among female customers'

query=generate_athena_query(message)

print(query)