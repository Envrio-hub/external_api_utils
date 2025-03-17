__version__="0.1.0"
__authors__=['Ioannis Tsakmakis']
__date_created__='2023-11-27'
__last_updated__='2024-10-29'

import requests, xmltodict
from datetime import datetime, timedelta
import pandas as pd

class addUPI():

    '''Initialization and Authendication Methods'''

    def __init__(self, credentials: dict, host_id:float = None, timeout:int = 360, mode:str = 't', version:str = '1.0') -> str:
        """
        Initialize addUPI session.

        :param credentials: Dictionary containing 'username' and 'password'.
        :param host_id: Unique host identifier generated at installation.
        :param timeout: Session duration, in seconds. Default is 360.
        :param mode: Response format, either 't' (ASCII text) or 'z' (compressed). Default is 't'.
        :param version: addUPI API version for communication. Default is '1.0'.
        """   
        self.headers = {'content-type': 'application/xml'}
        self.credentials = credentials

        params = {'function': 'login',
                  'user': self.credentials['username'],
                  'passwd': self.credentials['password'],
                  'host-id':host_id if host_id else None,
                  'timeout':timeout,
                  'mode':mode,
                  'version':version}
        
        response = requests.get(self.credentials['base_url'], params = params, headers = self.headers)

        if response.status_code == 200:
            self.session_id =  xmltodict.parse(response.text)["response"]["result"]["string"]
        else:
            raise requests.exceptions.HTTPError({"status_code":response.status_code,"message":xmltodict.parse(response.text)['response']})
          
    def log_out(self, mode:str = 't') -> None:
        """
        Terminate the current session.

        :param mode: Format of the response, either 't' (ASCII text) or 'z' (compressed). Default is 't'.
        """
        params = {
            'function': 'logout',
            'mode':mode
            }
        
        response = requests.get(self.credentials['base_url'], params = params, headers = self.headers)
        return response

    '''Configuration Methods'''

    def _make_configuration_authendicated_request(self, function: str, id: int = None, depth:int = None, df:str = 'iso8601',
                                                  flags:list = None, mode:str = 't', template:str = None, attrib:str = None,
                                                  cashe:str = 'v'):
       
        params = {
            'function':function,
            'session-id':self.session_id,
            'id':id,
            'depth':depth,
            'df':df,
            'flags':flags,
            'mode':mode,
            'tempalte':template,
            'attrib':attrib,
            'cashe':cashe
        }

        response = requests.get(self.credentials['base_url'], params = params, headers = self.headers)

        return {"status_code":response.status_code, function:xmltodict.parse(response.content)['response']}

    def get_config(self, node_id: int =None, depth:int = None, df:str = 'iso8601', flags:list = None, mode:str = 't') -> dict:
        """
        Retrieve configuration data for a specified node.

        :param node_id: ID of the node to retrieve configuration for. Defaults to root node if None.
        :param depth: Number of hierarchy levels to retrieve within the tree structure.
        :param df: Date format to use for any returned timestamps. 'iso8601' for standard date-time format, 
                   or 'time_t' for Unix timestamp (seconds since epoch). Default is 'iso8601'.
        :param flags: Specifies additional data to retrieve at each level, such as attributes ('a'), 
                      events ('e'), or functions ('f'). Can use multiple flags, e.g., ['a', 'f']. 
                      Default is None, returning only node information.
        :param mode: Format of the response; 't' for ASCII text, 'z' for compressed text. Default is 't'.

        :return: Parsed XML response with configuration details for the specified node.
        """
        return self._make_configuration_authendicated_request(function='getconfig', id=node_id, depth=depth, df=df, flags=flags, mode=mode)
    
    def get_template(self, template:str = None,  mode:str = 't') -> dict:
        """
        Retrieve a specific template or all available templates if no template name is provided.

        :param template: Name of the specific template to retrieve. If None, all templates known by the server are returned.
        :param mode: Format of the response; 't' for ASCII text, 'z' for compressed text. Default is 't'.

        :return: Parsed XML response with template details.
        """
        return self._make_configuration_authendicated_request(function='gettemplate', template=template, mode=mode)

    def get_attrib(self, node_id: int, attrib: str = None) -> dict:
        """
        Retrieve attributes for a specified node.

        :param node_id: ID of the node for which to retrieve attributes (mandatory).
        :param attrib: Name of the specific attribute to retrieve. If None, all attributes of the node are returned.

        :return: Parsed XML response containing attribute information for the specified node.
        """
        return self._make_configuration_authendicated_request(function='getattrib', id=node_id, attrib=attrib)

    '''Data Transfer Methods'''

    """The Data Transfer Methods may use one of more of the following Parameters

    :param session_id: is the result returned by the login function (mandatory).
    :type session_id: str
    :param id: The node’s ID. Default is the root tree.
    :type id: str, optional
    :param df: The date format. It can be iso8601 or time_t. If a certain date format is specified,
        then the answer must be returned using that format. Default is iso8601 is implied. The time_t format represents
        the number of seconds elapsed since 1 Jan 1970 0:0:0 relative to the UTC meridian. If the answer is known not to
        include date values, it can be omitted.
    :type df: str, optional
    :param da: The data anchor. It can be begin, end or offset. If missing, end is implied. This setting specifies whether the returned timestamps specifythe begin of the slot interval,
        end of the slot interval or offset of the measured value within the slot interval.
    :type da: ENUM
    :param date: The date of the newest stored slot on the client — the data returned will be strictly newer than this date.
        If missing, the last stored slot will be returned
    :type date: str
    :param slots: The number of slots the client is prepared to accept (per node). The server may return less slots
        that requested, but never more — if missing, default one slot is implied. If a server returns less slots for 
        a node than requested by the client, then the client assumes that there are no more data available for that node. 
        The maximum number of slots requested by a client must not be greater than the value of the getdataMaxSlots attribute
        of the server (the root node in a getconfig response). If a client request more slots that specified by the get
        dataMaxSlots attribute, the server will return an error.
    :type slots: int
    :param mode: Specifies the format of the response: 't' for ASCII text or 'z' for compressed text. Defaults to 't'.
    :type mode: str, optional
    :param cache: can be y or n and specifies if the data should be retrieved from the cache of
        the node or directly from the tag (if the tag supports immediate requests). Default is set to v (i.e. retrieve from cache).
    :type cashe: str, optional
    """
    def _make_data_transfer_authendicated_request(self, function: str, id: int = None, df:str = 'iso8601', 
                                                  da:str = 'end', date:str = datetime.now().strftime("%Y%m%dT%H:%M:%S"),
                                                  mode:str = 't', cashe:str = 'v', slots:int = 1):

        params = {
            'function':function,
            'session-id':self.session_id,
            'id':id,
            'df':df,
            'da':da,
            'date':date,
            'mode':mode,
            'cashe':cashe,
            'slots':slots
        }

        response = requests.get(self.credentials['base_url'], params = params, headers = self.headers)

        return {"status_code":response.status_code, function:xmltodict.parse(response.content)['response']}
    
    def get_sensor_data(self, sensor_id: str, start: float=(datetime.now()- timedelta(hours = 3)).timestamp(),
                        end: float=datetime.now().timestamp(), step: int=1800):
        
        response = self._make_data_transfer_authendicated_request(function='getdata', id=sensor_id,
                                                                  date=datetime.fromtimestamp(start).strftime("%Y%m%dT%H:%M:%S"),
                                                                  slots=int((end- start)/step))

        if response.status_code != 200:
            return {"status_code":response.status_code,'message':xmltodict.parse(response.text)['response']}
        
        if isinstance(xmltodict.parse(response.text)['response']['node'], list) and xmltodict.parse(response.text)['response']['node'].get('error'):
            return {"status_code":response.status_code, 'message':xmltodict.parse(response.text)['response']['node']['error']['@msg']}

        datadf = pd.DataFrame(xmltodict.parse(response.text)['response']['node']['v'])
        datadf.loc[0, '@t'] = datetime.strptime(datadf.iloc[0]['@t'], '%Y%m%dT%H:%M:%S')
        for i in range(1, len(datadf)):
            datadf.loc[i, '@t'] = datadf.loc[i-1, '@t'] + timedelta(seconds=int(datadf.loc[i, '@t']))
        data_dict = {"date_time":[i for i in datadf['@t'].values],"values":[i for i in datadf['#text'].values]}
        return {"status_code":response.status_code,"sensor_data":data_dict}
      
    def get_station_data(self, station_id: list, start: float=(datetime.now()- timedelta(hours = 3)).timestamp(),
                         end: float=datetime.now().timestamp(), step=1800):
        data_dict = {}

        response = self._make_data_transfer_authendicated_request(function='getdata', id=station_id,
                                                                  date=datetime.fromtimestamp(start).strftime("%Y%m%dT%H:%M:%S"),
                                                                  slots=int((end- start)/step))

        if response['status_code'] != 200:
            return {"status_code":response['status_code'],'message':response['getdata']}
        # Lists to store valid devices and devices with errors
        devices = [device for device in response['getdata']['node']]
        valid_devices = [valid for valid in devices if valid.get('v')]
        devices_with_errors = [with_error for with_error in devices if with_error.get('error')]
        # Prepare data dict
        for sensor in sensors_info:
            if [error for error in devices_with_errors if error['@id']==sensor['code']]:
                continue
            datadf = pd.DataFrame([valid for valid in valid_devices if valid['@id']==sensor['code']][0]['v'])
            datadf.loc[0, '@t'] = datetime.strptime(datadf.iloc[0]['@t'], '%Y%m%dT%H:%M:%S')
            for i in range(1, len(datadf)):
                datadf.loc[i, '@t'] = datadf.loc[i-1, '@t'] + timedelta(seconds=int(datadf.loc[i, '@t']))
            data = {"date_time":[i for i in datadf['@t'].values],"values":[i for i in datadf['#text'].values]}
            data_dict[sensor['measurement']] = data

        return {'status_code':200, "station_data":data_dict, "errors":devices_with_errors}