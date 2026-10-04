import requests
from Configs.API import API_KEY

def get_url_response(url,additional_params=None):
    try:
        api_key = API_KEY.get_api_key()
        if additional_params and "planetary/apod" not in url:
        # Convert API parameter names
            if "start_date" in additional_params:
                additional_params["startDate"] = additional_params.pop("start_date")
            if "end_date" in additional_params:
                additional_params["endDate"] = additional_params.pop("end_date")
        params = {
            "api_key":api_key
        }
        if additional_params:
            params.update(additional_params)
        # print("URl:",url)
        # print("Params:",params)
        response = requests.get(url=url,params=params)
        # print(response)
        data = response.json()
        status = response.status_code
        return data,status
    except Exception as e:
        print("Job failed with error:",e)