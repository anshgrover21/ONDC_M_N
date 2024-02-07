from fastapi import FastAPI, File, UploadFile, Depends , HTTPException
import pandas as pd 
import numpy as np
from mn_ondc import Graph
from typing import List,Dict , Set 
import sys 
import pickle 
import dill 
from google_cloud import google_cloud_bucket as gcb
from google.cloud import storage
from logger import logging 
from exception import CustomException 
import io 
# from memory_profiler import profile




app = FastAPI()

# def load_graph():
#     with open("artifacts/data.pkl","rb") as file_obj : 
#         return dill.load(file_obj) 

# graph = load_graph() 

graph = Graph() 

def get_graph() : 
    return graph


@app.get("/")
def root(graph : Graph = Depends(get_graph)):
    for i in graph.merchant_graph : 
        # if (len(graph.merchant_graph[i])) : 
        print ( i , graph.merchant_graph[i] )
    for j in graph.pincode_graph:
        # if  (len(graph.pincode_graph[j])) : 
        print ( j , graph.pincode_graph[j] ) 

@app.get("/search/")
async def get_pincodes(merchant_id: int = None, pincode: int = None, graph: Graph = Depends(get_graph)):
    try:
        if merchant_id is not None:
            return {"pincodes": list(graph.find_pincode(merchant_id))}
        elif pincode is not None:
            return {"merchants": list(graph.find_merchants(pincode))}
        else:
            raise HTTPException(status_code=400, detail="Missing parameter: merchant_id or pincode")
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))



# @app.get( "/search/")
# async def get_merchants( , graph : Graph = Depends(get_graph) ):
#     try : 
#         print ( graph.find_merchants(pincode) )
#         return {"merchants" : graph.find_merchants(pincode)}
#     except Exception as e : 
#         raise CustomException ( e , sys ) 

# @profile
async def create_graph(bucket_name : str , blob_name : str ):
    global graph 
    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(blob_name)
    try:
        blob_content = blob.download_as_string()
         
        blob_file = io.BytesIO(blob_content)
        # print ( blob_file  )


        for chunk in pd.read_csv(blob_file, iterator=True,header=None , chunksize=100, names=[f"col_{i}" for i in range(30000)] , na_values=["NaN"]  ):
            chunk.fillna(-1, inplace=True)
            # chunk.astype(int)
            for _, row in chunk.iterrows():
                merchant_id = row.iloc[0]
                if merchant_id not in graph.merchant_graph:
                    graph.merchant_graph[merchant_id] = set()
                for i in row.iloc[1:]:
                    if ( i == -1  ) :
                        break 
                    else :
                        graph.merchant_graph[merchant_id].add(i) 
                        if i not in graph.pincode_graph:
                            graph.pincode_graph[i] = set()
                        graph.pincode_graph[i].add( merchant_id )
        
        return {"message" : "Graph Created Successfully"}
                    
    except Exception as e:
        return {"Message" : "Unable to Create Graph" , 
                "Error" : e }
        raise CustomException ( e , sys ) 

@app.get("/create_graph")
async def revoke_create_graph():    
    await create_graph("mxndatabucket" , "dataset_100000.csv")
    


@app.post("/upload-csv/")
async def upload_csv(file: UploadFile = File(...) ):
    try : 
        file_contents = await file.read()  # Read the contents of the uploaded file as bytes
        file_contents_str = file_contents.decode('utf-8')  # Convert bytes to string
        # # print (file_contents_str)
        # gcb.upload_blob(file.file, "data/" + file.filename)
        gcb.upload_an_object( file.filename ,file_contents_str , file.content_type ) 
        return "file read successfully"
    except Exception as e : 
        raise CustomException(e , sys ) 
