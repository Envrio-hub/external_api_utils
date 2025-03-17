__version__="0.1.0"
__authors__=['Ioannis Tsakmakis']
__date_created__='2023-11-27'
__last_updated__='2024-10-29'

import requests
from datetime import datetime

class DavisApi:
    def __init__(self, credentials: dict):
        self.api_key = credentials['key']
        self.client_secret = credentials["secret"]

    def _make_request(self, endpoint: str, params: dict):
        """Helper method to make requests to the Davis API."""
        headers = {"X-Api-Secret": self.client_secret}
        response = requests.get(url=endpoint, params=params, headers=headers)
        if response.status_code == 200:
            return {"status_code": 200, "data": response.json()}
        return {"status_code": response.status_code, "message": response.json().get('message', 'Unknown error')}

    def get_stations(self):
        """Fetch all stations from the Davis API."""
        return self._make_request("https://api.weatherlink.com/v2/stations", {"api-key": self.api_key})

    def get_station_metadata(self, station_id):
        """Fetch metadata for a specific station."""
        return self._make_request(f"https://api.weatherlink.com/v2/stations/{station_id}", {"api-key": self.api_key})

    def get_sensor_catalog(self):
        """Fetch the sensor catalog."""
        return self._make_request("https://api.weatherlink.com/v2/sensor-catalog", {"api-key": self.api_key})

    def get_sensors(self):
        """Fetch all sensors."""
        return self._make_request("https://api.weatherlink.com/v2/sensors", {"api-key": self.api_key})

    def get_current(self, station_id):
        """Fetch current data for a specific station."""
        response = self._make_request(f"https://api.weatherlink.com/v2/current/{station_id}", {"api-key": self.api_key})
        if response["status_code"] == 200 and response["data"].get('sensors', [{}])[0].get('data'):
            data = self._parse_station_data(data=response["data"])
            return {"status_code": 200, "station_data": data}
        return response

    def get_historic(self, station_id: int, start: int, end: int):
        """Fetch historic data for a specific station and time range."""
        params = {"api-key": self.api_key, "start-timestamp": start, "end-timestamp": end}
        response = self._make_request(f"https://api.weatherlink.com/v2/historic/{station_id}", params)
        if response["status_code"] == 200 and response["data"].get('sensors', [{}])[0].get('data'):
            data = self._parse_station_data(data=response["data"])
            return {"status_code": 200, "station_data": data}
        return response

    def get_report(self, station_id):
        """Fetch ET report for a specific station."""
        return self._make_request(f"https://api.weatherlink.com/v2/report/et/{station_id}", {"api-key": self.api_key})

    def _parse_station_data(self, data: dict) -> dict:
        """Parse sensor data into structured output."""
        sensor_data = [
            sensor_set['data'] for sensor_set in data['sensors']
            if sensor_set.get('sensor_type') in [30, 4, 31]
        ]
        date_time = [datetime.fromtimestamp(step['ts']) for step in sensor_data[0]]
        return {
            "air_temperature": {"date_time": date_time, "values": [(step['temp_out'] - 32) * 5 / 9 for step in sensor_data[0]]},
            "relative_humidity": {"date_time": date_time, "values": [step['hum_out'] for step in sensor_data[0]]},
            "wind_speed": {"date_time": date_time, "values": [step['wind_speed_avg'] for step in sensor_data[0]]},
            "wind_speed_of_gust": {"date_time": date_time, "values": [step['wind_speed_hi'] for step in sensor_data[0]]},
            "wind_from_direction": {"date_time": date_time, "values": [step['wind_dir_of_prevail'] for step in sensor_data[0]]},
            "downwelling_shortwave_flux_in_air": {"date_time": date_time, "values": [step['solar_rad_avg'] for step in sensor_data[0]]},
            "lwe_thickness_of_precipitation_amount": {"date_time": date_time, "values": [step['rainfall_mm'] for step in sensor_data[0]]},
            "water_evapotranspiration_amount": {"date_time": date_time, "values": [step['et'] for step in sensor_data[0]]}
        }