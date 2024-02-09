import dill 
from exception import CustomException 
import sys 

import os 

def save_object(obj, destination: str):
    try:
        with open(destination, "wb") as file_obj:
            dill.dump(obj, file_obj)
    except Exception as e:
        raise CustomException(e, sys)
    

def load_object(filepath): 
    try : 
        with open(filepath , "rb" ) as file_obj : 
            return dill.load(file_obj)
    except Exception as e : 
        raise CustomException (  e , sys ) 
def delete_object(filepath) : 
    try : 
        os.remove(filepath)
    except Exception as e : 
        raise CustomException( e , sys ) 
    