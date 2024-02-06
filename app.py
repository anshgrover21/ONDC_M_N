from fastapi import FastAPI, File, UploadFile, Depends 
import pandas as pd 
import numpy as np
from mn_ondc import Graph
from typing import List,Dict , Set 
import sys 
import pickle 
import dill 
app = FastAPI()

def load_graph():
    with open("data/data.pkl","rb") as file_obj : 
        return dill.load(file_obj) 

graph = load_graph() 


def get_graph() : 
    return graph


@app.get("/")
def root(graph : Graph = Depends(get_graph)):
    for i in graph.merchant_graph : 
        if (len(graph.merchant_graph[i])) : print ( i , graph.merchant_graph[i] )
    for j in graph.pincode_graph:
        if  (len(graph.pincode_graph[j])) : print ( j , graph.pincode_graph[j] ) 

@app.get("/search/{merchant_id}")
async def get_pincodes(merchant_id : int, graph : Graph = Depends(get_graph) ):
    try : 
        # print ( graph.find_pincode(merchant_id))
        return {"pincodes" : graph.find_pincode(merchant_id) }
    except Exception as e : 
        print ( e ) 



@app.get( "/search/{pincode}")
async def get_merchants(pincode : int , graph : Graph = Depends(get_graph) ):
    try : 
        print ( graph.find_merchants(pincode) )
        return {"merchants" : graph.find_merchants(pincode)}
    except Exception as e : 
        print ( e ) 


def read_csv(file: UploadFile = File(...), chunk_size: int = 1000):
    global graph 
    try:
        # Initialize graph if not already created
        if not graph:
            graph = Graph()  # Assuming Graph is a class you have defined

        # Use pandas to read the CSV file in chunks
        csv_stream = file.file

        # Iterate through chunks
        for chunk in pd.read_csv(csv_stream, iterator=True,header=None , dtype = int, chunksize=chunk_size):

            for _, row in chunk.iterrows():

                graph.merchant_graph[int(row[0])].update(row[1:].values)
                for i in row[1:].values:
                    graph.pincode_graph[i].add( row[0] )
    except Exception as e:
        print(e)
    finally:
        # Close the file stream
        file.file.close()

    return

@app.post("/upload-csv/")
async def upload_csv(file: UploadFile = File(...) ):
    read_csv(file) 
    return "file read successfully"
