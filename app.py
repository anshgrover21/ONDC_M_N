from fastapi import FastAPI, File, UploadFile, Depends , HTTPException
import pandas as pd 
from mn_ondc import Graph
from typing import List,Dict , Set 
import sys  
from google.cloud import storage
from google_cloud import google_cloud_bucket as gcb
from logger import logging 
from exception import CustomException 
import io 
import os  
from utils import load_object,save_object,delete_object





app = FastAPI()
    
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
async def search_operation(merchant_id: int = None, pincode: int = None, graph: Graph = Depends(get_graph)):
    try:
        if merchant_id is not None:
            tmp = graph.find_pincode(merchant_id) 
            if ( tmp ) :
                return {"pincodes": graph.find_pincode(merchant_id)}
            else : 
                raise HTTPException(status_code=304 , detail = f"No Pincode found for the  merchant_id : {merchant_id} ")
        elif pincode is not None:
            tmp = graph.find_merchants(pincode) 
            if ( tmp ) :
                return {"merchants": graph.find_merchants(pincode) }
            else : 
                raise HTTPException (status_code= 304 , detail = f"No merchants found for the  pincode : {pincode} ")
        else:
            raise HTTPException(status_code=403, detail="Missing parameter: merchant_id or pincode")
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    


async def create_graph(bucket_name : str , blob_name : str ):
    global graph 
    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(blob_name)
    try:
        blob_content = blob.download_as_string()
         
        blob_file = io.BytesIO(blob_content)
        # print ( blob_file  )

        logging.info("Creating Graph")
        for chunk in pd.read_csv(blob_file, iterator=True,header=None , chunksize=500, names=[f"col_{i}" for i in range(30000)] , na_values=["NaN"]  ):
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
        logging.info("Graph Created Successfully") 

        return {"message" : "Graph Created Successfully"}
                    
    except Exception as e:
        raise HTTPException (status_code= 500  , detail = str(e) )
        

# @app.get("/sample_dataset")
# async def list_dataset():
#     logging.info("Fetching List of Blobs from GCP")
#     try : 
#         return {
#             "datasets" : gcb.list_blob_from_bucket()
#         }
#     except Exception as e : 
#         raise HTTPException ( status_code = 500  , detail = str(e))

@app.post("/create_graph")
async def get_sampled_graph(filename : str , graph : Graph = Depends(get_graph)): 
    try :
        logging.info(f"Create Graph is called on filename {filename} ")

        await create_graph("mxndatabucket" , filename)
        save_path  = os.path.join("artifacts/","graph.pkl")
        save_object(graph , save_path )
        await gcb.upload_blob("artifacts/graph.pkl" , "sample/graph.pkl" )
        delete_object("artifacts/graph.pkl")


        logging.info("Sampled Graph Retreived")
    except Exception as e : 
        raise HTTPException(status_code=500, detail=str(e))
    


@app.post("/upload-csv/")
async def upload_csv(file: UploadFile = File(...) ):
    try : 
        logging.info("Uploading File to GCP") 

        file_contents = await file.read()  
        file_contents_str = file_contents.decode('utf-8')  
        gcb.upload_an_object( file.filename ,file_contents_str , file.content_type )

        logging.info(f"File : {file.filename}Upload to GCP completed") 
        return {"message" : "File Read Successfully"}
    except Exception as e : 
        raise CustomException(e , sys ) 




@app.get("/take_sample_graph")
async def sample_graph () : 
    try : 
        logging.info("Taking sample Graph from the Bucket ")
        check = await gcb.download_blob("sample/graph.pkl" , "artifacts/graph.pkl")

        if ( check ) : 
            
            global graph  
            graph = load_object ("artifacts/graph.pkl")

            delete_object("artifacts/graph.pkl")
        else :
            raise HTTPException (status_code= 403 , detail = "Unable to Download Sample Graph ")
        logging.info("Sample Graph Retrieved Successfully")
    except Exception as e : 
        raise HTTPException( status_code= 403 , detail= str ( e ) )
    