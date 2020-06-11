import pandas as pd
from clickhouse_driver import Client
import numpy as np
import numbers

class PyhouseConnector(object):

    def get_clickhouse_client(self, host, port, user, password):
        return Client(host=host, port=port, user=user, password=password)
    
    def __init__(self, host, port, user, password):
        self.cur_client = self.get_clickhouse_client(host, port, user, password)

    def convert_arg(self, arg):
        if isinstance(arg, str):
            return {'type': 'string', 'value': arg}
        elif isinstance(arg, numbers.Number):
            return {'type': 'number', 'value': str(arg)}
        elif isinstance(arg, (list, np.ndarray)):
            arg = np.array(arg)
            if arg.dtype.kind in ['U', 'S']:
                return {'type': 'array_string', 'value': ', '.join(list(map(lambda x: "'" + x + "'", arg)))}
            else:
                return {'type': 'array_not_string', 'value': ', '.join(list(map(lambda x: str(x), arg)))}
        else:
            return None

    def construct_query(self, query, kwargs):
        for arg in kwargs:
            query = query.replace('@' + arg, self.convert_arg(kwargs[arg])['value'])
            return query
       
    def execute_sql(self, query, **kwargs):
        query = self.construct_query(query, kwargs)
        result = self.cur_client.execute(query, with_column_types=True)
        data_frame = pd.DataFrame(result[0], columns=np.array(result[1])[:,0])
        return data_frame