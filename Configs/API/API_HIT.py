import requests
from Configs.API import API_KEY

def get_url_response(url,additional_params=None):
    api_key = API_KEY.get_api_key()
    params = {
        "api_key":api_key
    }
    if additional_params:
        params.update(additional_params)
    print("URl:",url)
    print("Params:",params)
    response = requests.get(url=url,params=params)
    print(response)
    data = response.json()
    status = response.status_code
    return data,status