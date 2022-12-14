import requests
import json


def xyz():
        base_url_1 = 'https://api.icmr.org.in/covid_data/index.php/login'
        base_url_2 = 'https://api.icmr.org.in/covid_data/index.php/covid-data'

        data = {"username": "ranjeet.singh.mashon",
                "password": "ranjeet.singh^&$@icmr1239"}

        icmr_data = requests.post(base_url_1 , data= json.dumps(data))
        icmr_response = icmr_data.json()
        icmr_data.raise_for_status()
        print(icmr_response['token'])
        return (icmr_response['token'])


