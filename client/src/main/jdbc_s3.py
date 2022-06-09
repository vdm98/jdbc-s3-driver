import json
import logging
import os
import jaydebeapi

from redash.query_runner import *
from redash.settings import parse_boolean
from redash.utils import JSONEncoder

logger = logging.getLogger(__name__)
types_map = {
    jaydebeapi.STRING: TYPE_STRING,
    jaydebeapi.TEXT: TYPE_STRING,
    jaydebeapi.NUMBER: TYPE_INTEGER,
    jaydebeapi.FLOAT: TYPE_FLOAT,
    jaydebeapi.DECIMAL: TYPE_FLOAT,
    jaydebeapi.DATE: TYPE_DATE,
    jaydebeapi.DATETIME: TYPE_DATETIME,
    jaydebeapi.ROWID: TYPE_STRING,
}

class Jdbc_s3(BaseSQLQueryRunner):
    noop_query = "SELECT 1"

    @classmethod
    def configuration_schema(cls):
        schema = {
            'type': 'object',
            'properties': {
                'host': {
                    'type': 'string',
                },
                'port': {
                    'type': 'number',
                    'default': 3306,
                },
                'user': {
                    'type': 'string'
                },
                'password': {
                    'type': 'string',
                },
                'database': {
                    'type': 'string',
                },
            },
            "order": ['host', 'port', 'user', 'password', 'database'],
            'required': ['host', 'port', 'user', 'database'],
            'secret': ['password']
        }

        return schema

    @classmethod
    def name(cls):
        return "jdbc_s3"

    @classmethod
    def enabled(cls):
        try:
            import jaydebeapi
        except ImportError:
            return False

        return True

    def _get_tables(self, schema):
        print("----- get tables fom model", flush=True)
        with open('./redash/query_runner/s3model.json') as f:
           model = json.load(f)
        tables = model['schemas'][0]['operand']['tableSpecs']
        for table in tables:
           table_name = table['label']
           schema[table_name] = {'name': table_name, 'columns': []}
           columns = table['columns']
           for column in columns:
               schema[table_name]['columns'].append(column['label'])
        return schema.values()

    def run_query(self, query, user):
        import jaydebeapi

        connection = None
        try:
            host = self.configuration.get('host', '')
            port = self.configuration.get('port', '')
            user = self.configuration.get('user', '')
            password = self.configuration.get('password', '')
            database = self.configuration.get('database', '')
            jclassname = 'org.apache.calcite.jdbc.Driver'
            jar_path = '/app/redash/query_runner/jdbc-s3-driver.jar'

            with open('./redash/query_runner/s3model.json') as f:
                model_j = json.load(f)
            model = json.dumps(model_j)

            url = "jdbc:calcite:model=inline:" + model_s
            driver_args = {'user': user, 'password': password}
            connection = jaydebeapi.connect(jclassname, url, driver_args, jar_path)
            cursor = connection.cursor()
            logger.info("JDBC running query: %s", query)
            cursor.execute(query)

            data = cursor.fetchall()

            if cursor.description is not None:
                columns = self.fetch_columns([(i[0], types_map.get(i[1], None)) for i in cursor.description])
                rows = [dict(zip((c['name'] for c in columns), row)) for row in data]

                data = {'columns': columns, 'rows': rows}
                json_data = json.dumps(data, cls=JSONEncoder)
                error = None
            else:
                json_data = None
                error = "No data was returned."

            cursor.close()
        except jaydebeapi.Error as e:
            json_data = None
            error = str(e.message)
        except KeyboardInterrupt:
            cursor.close()
            error = "Query cancelled by user."
            json_data = None
        finally:
            if connection:
                connection.close()

        return json_data, error

register(Jdbc_s3)