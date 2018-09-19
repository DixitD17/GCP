
import sys
import json

# [START functions_pubsub_trigger_test]
def pubsub_trigger_test(data, context):
    """Background Cloud Function to be triggered by Pub/Sub.
    Args:
         data (dict): The dictionary with data specific to this type of event.
         context (google.cloud.functions.Context): The Cloud Functions event
         metadata.
    """
    import base64

    if 'data' in data:
        msg = base64.b64decode(data['data']).decode('utf-8')
        parseddata = json.loads(msg)
        upc = parseddata["sku_upc_nbr"]
		
		
    else:
        msg = 'empty'
        upc = 'empty'
    print(f'UPC : {upc} have been extracted out of message {msg}')
# [END functions_pubsub_trigger_test]



def hello_error_1(request):
    # This WILL be reported to Stackdriver Error Reporting,
    # and WILL terminate the function
    raise RuntimeError('I failed you')

    # [END functions_helloworld_error]


def hello_error_2(request):
    
    # WILL NOT be reported to Stackdriver Error Reporting, but will show up
    # in logs
    import logging
    print(RuntimeError('I failed you (print to stdout)'))
    logging.warn(RuntimeError('I failed you (logging.warn)'))
    logging.error(RuntimeError('I failed you (logging.error)'))
    sys.stderr.write('I failed you (sys.stderr.write)\n')

    # This WILL be reported to Stackdriver Error Reporting
    from flask import abort
    return abort(500)
    # [END functions_helloworld_error]
