__version__="0.1.0"
__authors__=['nkokkos@envrio.org', 'Ioannis Tsakmakis']
__date_created__='2023-11-27'
__last_updated__='2024-10-29'

import requests
from datetime import datetime

class MetricaApi:
    def __init__(self, credentials: dict):
        self.base_url = credentials['base_url']
        self.username = credentials['username']
        self.password = credentials['password']

    def authendicate(self):
        """Authenticate to the Metrica API and retrieve a token."""
        response = requests.post(f'{self.base_url}/token', headers={"Content-Type": "application/json"},
                                 json={"email": self.username, "key": self.password})
        if response.status_code == 200:
            self.token = response.json().get('token')
            return {"status_code": 200, "message": "Successful authentication"}
        return {"status_code": response.status_code, "message": response.json()['message']}

    def _make_authenticated_request(self, endpoint: str, payload: dict):
        """Helper method for making authenticated requests to Metrica API."""
        auth = self.authendicate()
        if auth['status_code'] == 200:
            headers = {"content-type": "application/json", "Authorization": f'Bearer {self.token}'}
            response = requests.post(url=endpoint, headers=headers, json=payload)
            if response.status_code == 200 and response.json().get('code') == '200':
                return {"status_code": 200, "data": response.json()[list(response.json().keys())[1]]}
            return {"status_code": response.json()['code'], "message": response.json().get('message', 'Unknown error')}
        return auth

    def get_station(self, station_id: str):
        """Fetch specific station details."""
        return self._make_authenticated_request(f'{self.base_url}/stations', {"station_id": station_id})

    def get_stations(self):
        """Fetch all stations."""
        return self._make_authenticated_request(f'{self.base_url}/stations', {})

    def get_sensors(self, station_id: str):
        """Fetch sensors for a specific station."""
        return self._make_authenticated_request(f'{self.base_url}/sensors', {"station_id": station_id})

    def get_station_data(self, sensors_info: list, start: float, end: float, last_communication: float = None):
        '''Get data from all the station monitoring devices'''
        data = {}
        dateto, timeto = datetime.fromtimestamp(end).strftime('%Y-%m-%d'), datetime.fromtimestamp(end).strftime('%H:%M')
        if start:
            datefrom, timefrom = datetime.fromtimestamp(start).strftime('%Y-%m-%d'), datetime.fromtimestamp(start).strftime('%H:%M')   
        else:
            datefrom, timefrom = datetime.fromtimestamp(last_communication).strftime('%Y-%m-%d'), datetime.fromtimestamp(last_communication).strftime('%H:%M')
        json_body = {
                    'datefrom': datefrom,
                    'dateto': dateto,
                    'timefrom': timefrom,
                    'timeto': timeto,
                    'sensor': [sensor['code'] for sensor in sensors_info]
                    }
        response = self._make_authenticated_request(f'{self.base_url}/measurements', json_body=json_body)
        if response.json().get('scalar'):
            return  {"status_code":response.status_code, "message":response.json()['scalar']}
        elif response.status_code==200 and response.json().get('measurements')[0]['values']:
            for sensor in sensors_info:
                dict = [obj for obj in response.json()['measurements'] if obj.get('sensor') == sensor['name']['el']]
                sensor_data = {"date_time":[datetime.strptime(f'{data_step["mdate"]}T{data_step["mtime"]}','%Y-%m-%dT%H:%M:%S') for data_step in dict[0]['values']],
                                "values":[data_step['mvalue'] for data_step in dict[0]['values']]}
                data[sensor['measurement']] = sensor_data
        elif response.status_code==200 and not response.json().get('measurements')[0]['values']:
            return {"status_code":204,"station_data":response.json().get('measurements')[0]['values']}
        
        return {"status_code":200,"station_data":data}

    def get_sensor_data(self, sensor_info: list, start: float, end: float, last_communication: float = None):
        
        dateto, timeto = datetime.fromtimestamp(end).strftime('%Y-%m-%d'), datetime.fromtimestamp(end).strftime('%H:%M')
        if start:
            datefrom, timefrom = datetime.fromtimestamp(start).strftime('%Y-%m-%d'), datetime.fromtimestamp(start).strftime('%H:%M')   
        else:
            datefrom, timefrom = datetime.fromtimestamp(last_communication).strftime('%Y-%m-%d'), datetime.fromtimestamp(last_communication).strftime('%H:%M')
        json_body = {
                    'datefrom': datefrom,
                    'dateto': dateto,
                    'timefrom': timefrom,
                    'timeto': timeto,
                    'sensor': [sensor_info.MonitoredParameters.code]
                    }
        response = self._make_authenticated_request(f'{self.base_url_metrica}/measurements', json_body=json_body)
        if response.json().get('scalar'):
            return  {"status_code":response.status_code, "message":response.json()['scalar']}
        elif response.status_code==200 and response.json().get('measurements')[0]['values']:
                dict = [obj for obj in response.json()['measurements'] if obj.get('sensor') == sensor['name']['el']]
                sensor_data = {"date_time":[datetime.strptime(f'{data_step["mdate"]}T{data_step["mtime"]}','%Y-%m-%dT%H:%M:%S') for data_step in dict[0]['values']],
                                "values":[data_step['mvalue'] for data_step in dict[0]['values']]}
                return {"status_code":200,"station_data":{sensor_info.MonitoredParameters.measurement:sensor_data}}
        elif response.status_code==200 and not response.json().get('measurements')[0]['values']:
            return {"status_code":204,"station_data":response.json().get('measurements')[0]['values']}