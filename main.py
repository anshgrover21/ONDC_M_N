import uvicorn 

def run_server() : 
    uvicorn.run("app:app" , port = 8080, reload=True ) 

if __name__ == "__main__" :
    run_server() 