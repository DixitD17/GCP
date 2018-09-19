# [START function_read_spanner_single_three]
def function_read_spanner_single_three(data, context):
    """Background Cloud Function to be triggered by Pub/Sub.
    Args:
         data (dict): The dictionary with data specific to this type of event.
         context (google.cloud.functions.Context): The Cloud Functions event
         metadata.
    """
    instancename = 'hubpoc3node'
    function_read_spanner_generic(data, instancename)

    print('function completed')


# [END function_read_spanner_single_three]

# [START function_read_spanner_single_six]
def function_read_spanner_single_six(data, context):
    """Background Cloud Function to be triggered by Pub/Sub.
    Args:
         data (dict): The dictionary with data specific to this type of event.
         context (google.cloud.functions.Context): The Cloud Functions event
         metadata.
    """
    instancename = 'hubpoc6node'
    function_read_spanner_generic(data, instancename)

    print('function completed')


# [END function_read_spanner_single_six]

# [START function_read_spanner_multi_three]
def function_read_spanner_multi_three(data, context):
    """Background Cloud Function to be triggered by Pub/Sub.
    Args:
         data (dict): The dictionary with data specific to this type of event.
         context (google.cloud.functions.Context): The Cloud Functions event
         metadata.
    """
    instancename = 'hubpoc3mnode'
    function_read_spanner_generic(data, instancename)

    print('function completed')


# [END function_read_spanner_multi_three]

# [START function_read_spanner_multi_six]
def function_read_spanner_multi_six(data, context):
    """Background Cloud Function to be triggered by Pub/Sub.
    Args:
         data (dict): The dictionary with data specific to this type of event.
         context (google.cloud.functions.Context): The Cloud Functions event
         metadata.
    """
    instancename = 'hubpoc6mnode'
    function_read_spanner_generic(data, instancename)

    print('function completed')


# [END function_read_spanner_multi_six]

def query_data_with_index(
        instance_id, database_id, start_title='Aardvark', end_title='Goo'):
    """Queries sample data from the database using SQL and an index.

    The index must exist before running this sample. You can add the index
    by running the `add_index` sample or by running this DDL statement against
    your database:

        CREATE INDEX AlbumsByAlbumTitle ON Albums(AlbumTitle)

    This sample also uses the `MarketingBudget` column. You can add the column
    by running the `add_column` sample or by running this DDL statement against
    your database:

        ALTER TABLE Albums ADD COLUMN MarketingBudget INT64

    """
    from google.cloud.spanner_v1.proto import type_pb2

    spanner_client = spanner.Client()
    instance = spanner_client.instance(instance_id)
    database = instance.database(database_id)

    params = {
        'start_title': start_title,
        'end_title': end_title
    }
    param_types = {
        'start_title': type_pb2.Type(code=type_pb2.STRING),
        'end_title': type_pb2.Type(code=type_pb2.STRING)
    }

    with database.snapshot() as snapshot:
        results = snapshot.execute_sql(
            "SELECT AlbumId, AlbumTitle, MarketingBudget "
            "FROM Albums@{FORCE_INDEX=AlbumsByAlbumTitle} "
            "WHERE AlbumTitle >= @start_title AND AlbumTitle < @end_title",
            params=params, param_types=param_types)

        for row in results:
            print(
                u'AlbumId: {}, AlbumTitle: {}, '
                'MarketingBudget: {}'.format(*row))
# [END spanner_query_data_with_index]


# [START function_read_spanner_generic]
def function_read_spanner_generic(data, instancename):
    """Background Cloud Function to be triggered by Pub/Sub.
    Args:
         data (dict): The dictionary with data specific to this type of event.
         instancename : Spanner instance name.
    """
    import base64

    if 'data' in data:
        msg = base64.b64decode(data['data']).decode('utf-8')
        parseddata = json.reads(msg)
        productid: int = parseddata["product_id"]
        upc: int = parseddata["sku_upc_nbr"]
        pricelevel = parseddata["fulfillment"]["price_level"]

        spanner_client = spanner.Client()
        instance = spanner_client.instance(instancename)
        database = instance.database('inventory')
		params = {
        'productid': productid,
		'upc': upc,
        'pricelevel': pricelevel
		}
		param_types = {
			'productid': type_pb2.Type(code=type_pb2.INT64),
			'upc': type_pb2.Type(code=type_pb2.INT64),
			'pricelevel': type_pb2.Type(code=type_pb2.INT64)
		}
        count: int = 0

        try:
            while count < 500:
                with database.snapshot() as snapshot:
                    keyset = spanner.KeySet(all_=True)
                    results = snapshot.read(
                        table='availability_by_productid_upc',
                        columns=('SingerId', 'AlbumId', 'AlbumTitle',),
                        keyset=keyset, )

                    for row in results:
                        print(u'SingerId: {}, AlbumId: {}, AlbumTitle: {}'.format(*row))
                count += 1
        except Exception as e:
            print(f' Exception occurred : {e} ')

        print(f' Table has been update for UPC : {upc} and message {msg}')

    else:
        print('No data to add')

    print('spanner generic read function completed')

# [END function_read_spanner_generic]
