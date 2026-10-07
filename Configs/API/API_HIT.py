import requests
from Configs.API import API_KEY

def get_url_response(url,additional_params=None):
    try:
        additional_params_cpy = additional_params.copy() if additional_params else {}
        api_key = API_KEY.get_api_key()
        if additional_params_cpy and "https://ccmc.gsfc.nasa.gov/DONKI-API/get/CME"  in url:
        # Convert API parameter names
            if "start_date" in additional_params_cpy:
                additional_params_cpy["startDate"] = additional_params_cpy.pop("start_date")
            if "end_date" in additional_params_cpy:
                additional_params_cpy["endDate"] = additional_params_cpy.pop("end_date")
        params = {
            "api_key":api_key
        }
        if additional_params_cpy:
            params.update(additional_params_cpy)
        # print("URl:",url)
        # print("Params:",params)
        response = requests.get(url=url,params=params)
        # print(response)
        data = response.json()
        status = response.status_code
        return data,status
    except Exception as e:
        print("Job failed with error:",e)