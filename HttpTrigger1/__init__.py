import logging
import requests
import azure.functions as func
from datetime import datetime, timedelta

def func_LineNotify(Message,LineToken ='XVDGomv0AlT1oztR2Ntyad7nWUYvBWU7XLHPREQYm6e'): #'qKZiexdyq6Ma5L0LH4b6kQEydQeHZF2pGG8DFHCINrs'):
    url  = "https://notify-api.line.me/api/notify"
    msn = {'message':Message}
    LINE_HEADERS = {"Authorization":"Bearer " + LineToken}
    session  = requests.Session()
    response =session.post(url, headers=LINE_HEADERS, data=msn)
    print(Message)
    response = Message
    return response 

def main(req: func.HttpRequest) -> func.HttpResponse:
    logging.info('Python HTTP trigger function processed a request.')

    name = req.params.get('name')
    if not name:
        try:
            req_body = req.get_json()
        except ValueError:
            pass
        else:
            name = req_body.get('name')
    current_date = datetime.now()

    if name:
        func_LineNotify(current_date)
        return func.HttpResponse(f"Hello, {name}. This HTTP triggered function executed successfully.")
    else:
        return func.HttpResponse(
             "This HTTP triggered function executed successfully. Pass a name in the query string or in the request body for a personalized response.",
             status_code=200
        )
