from datetime import datetime
from flask import Flask, render_template, request, redirect, url_for, send_from_directory
app = Flask(__name__)


@app.route('/')
def index():
    from azure.storage.blob import BlobServiceClient
    import pandas as pd
    import time
    import json

    STORAGEACCOUNTURL= "https://catalentpocstorageacc.blob.core.windows.net/"
    STORAGEACCOUNTKEY= "cJRSHfm3XJnNAmgYM45eMVX2K4Wyr1zxEmhcQCfarFlK7KntG/bb/pZBvTn/FXUAK8CUOSX/ur4J+AStgo0IZQ=="
    CONTAINERNAME= "pocdata"
    BLOBNAME= "0_e117c68ea3a24925b7ef2580fd273e97_1.json"
    LOCALFILENAME="0_e117c68ea3a24925b7ef2580fd273e97_1.json"

    #download from blob
    t1=time.time()
    blob_service_client_instance = BlobServiceClient(account_url=STORAGEACCOUNTURL, credential=STORAGEACCOUNTKEY)
    blob_client_instance = blob_service_client_instance.get_blob_client(CONTAINERNAME, BLOBNAME, snapshot=None)
    with open(LOCALFILENAME, "wb") as my_blob:
        blob_data = blob_client_instance.download_blob()
        blob_data.readinto(my_blob)
        print(blob_data," \n\nBLOB_DATA\n\n", my_blob,"\n\nMY_BLOB\n\n")
    t2=time.time()
    print(("It takes %s seconds to download "+BLOBNAME) % (t2 - t1))

    dataframe_blobdata = pd.read_csv(LOCALFILENAME, on_bad_lines='skip')
    print('the size of the data is: %d rows and  %d columns' % dataframe_blobdata.shape)
    head_data=dataframe_blobdata.head(10)
    return str(head_data)


@app.route('/', methods=['POST'])
def hello():
   name = request.form.get('name')

   if name:
       print('Request for hello page received with name=%s' % name)
       return render_template('hello.html', name = name)
   else:
       print('Request for hello page received with no name or blank name -- redirecting')
       return redirect(url_for('index'))

@app.route('/name', methods=['POST'])
def return_data():
    from azure.storage.blob import BlobServiceClient
    import pandas as pd
    import time
    import json

    STORAGEACCOUNTURL= "https://catalentpocstorageacc.blob.core.windows.net/"
    STORAGEACCOUNTKEY= "cJRSHfm3XJnNAmgYM45eMVX2K4Wyr1zxEmhcQCfarFlK7KntG/bb/pZBvTn/FXUAK8CUOSX/ur4J+AStgo0IZQ=="
    CONTAINERNAME= "pocdata"
    BLOBNAME= "0_e117c68ea3a24925b7ef2580fd273e97_1.json"
    LOCALFILENAME="0_e117c68ea3a24925b7ef2580fd273e97_1.json"

    #download from blob
    t1=time.time()
    blob_service_client_instance = BlobServiceClient(account_url=STORAGEACCOUNTURL, credential=STORAGEACCOUNTKEY)
    blob_client_instance = blob_service_client_instance.get_blob_client(CONTAINERNAME, BLOBNAME, snapshot=None)
    with open(LOCALFILENAME, "wb") as my_blob:
        blob_data = blob_client_instance.download_blob()
        blob_data.readinto(my_blob)
        print(blob_data," \n\nBLOB_DATA\n\n", my_blob,"\n\nMY_BLOB\n\n")
    t2=time.time()
    print(("It takes %s seconds to download "+BLOBNAME) % (t2 - t1))

    dataframe_blobdata = pd.read_csv(LOCALFILENAME, on_bad_lines='skip')
    print('the size of the data is: %d rows and  %d columns' % dataframe_blobdata.shape)
    head_data=dataframe_blobdata.all().to_string()
    return head_data

if __name__ == '__main__':
   app.run()