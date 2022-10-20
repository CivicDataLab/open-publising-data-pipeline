import json

import requests


def resource_query(res_id):
    query = f"""
    {{
      resource(resource_id: {res_id}) {{
        id
        title
        description
        issued
        modified
        status
        masked_fields
        dataset {{
          id
          title
          description
          issued
          remote_issued
          remote_modified
          period_from
          period_to
          update_frequency
          modified
          status
          remark
          funnel
          action
          dataset_type
        }}
        schema {{
          id
          key
          format
          description
        }}
        file_details {{
          format
          file
          remote_url
        }}
      }}
    }}
    """
    headers = {}  # {"Authorization": "Bearer YOUR API KEY"}
    request = requests.post('https://idpbe.civicdatalab.in/graphql', json={'query': query}, headers=headers)
    return request.text


def create_resource(resource_name, description, schema, file_format, files):
    query = f"""mutation 
        mutation_create_resource($file: Upload!) 
        {{create_resource(
                    resource_data: {{ title:"{resource_name}", description:"{description}",    
                    dataset: "8", status : "",
                    schema: {schema}, file_details:{{format: "{file_format}", file: $file,  remote_url: ""}}
                    }})
                    {{
                    resource {{ id }}
                    }}
                    }}
                    """
    print(query)
    variables = {"file": None}
    map = json.dumps({"0": ["variables.file"]})
    operations = json.dumps({
        "query": query,
        "variables": variables,
        "operationName": "mutation_create_resource"
    })
    try:
        response = requests.post('https://idpbe.civicdatalab.in/graphql', data={"operations": operations,
                                                                                "map": map}, files=files)
        response_json = json.loads(response.text)
        return response_json
    except:
        return None


def update_resource(res_details, file_format, schema, files):
    variables = {"file": None}

    map = json.dumps({"0": ["variables.file"]})
    query = f"""
                    mutation($file: Upload!) {{update_resource(resource_data: {{
                    id:{res_details['data']['resource']['id']}, 
                    title:"{res_details['data']['resource']['title']}", 
                    description:"{res_details['data']['resource']['description']}",   
                    dataset:"{res_details['data']['resource']['dataset']['id']}",
                    status:"{res_details['data']['resource']['status']}",  
                    file_details:{{ format:"{file_format}", file:$file, 
                    remote_url:"{res_details['data']['resource']['file_details']['remote_url']}" }},
                    schema:{schema},
                    }})
                    {{
                    success
                    errors
                    resource {{ id }}
                }}
                }}"""

    print(query)
    operations = json.dumps({
        "query": query,
        "variables": variables
    })
    headers = {}
    try:
        response = requests.post('https://idpbe.civicdatalab.in/graphql', data={"operations": operations, "map": map},
                                 files=files, headers=headers)
        print(response)
        response_json = json.loads(response.text)
        return response_json
    except:
        return None