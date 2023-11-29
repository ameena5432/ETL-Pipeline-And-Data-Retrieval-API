# Create an API endpoint, which takes "key" as input, query the table 
# And return the 90 days worth of data in a JSON response

from fastapi import FastAPI ,HTTPException
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from datetime import datetime , timedelta
from json_to_postgres import BookData

app = FastAPI()

db_url = "postgresql+pg8000://Datapipeline_DB_user:Datapipeline_DB_pwd@0.0.0.0:5432/Datapipeline_DB"

@app.post("/get_data")
def get_data(key:str):
    try:
        engine = create_engine(db_url)  
        Session = sessionmaker(bind=engine)
        session = Session()

        ninety_days_ago = datetime.now()-timedelta(days=90)
        result = session.query(BookData).filter(BookData.key == key ,BookData.date_rec_inserted >= ninety_days_ago).all()

        session.close()

        if result is None:
            print(f"result not found for key : {key}")
            raise HTTPException(status_code=404 , detail = 'Data not found')
        

        data_list = [{"id":item.id,"key":item.key,"date_rec_inserted":int(item.date_rec_inserted.tiemstamp()),"title":item.title,"last_modified_i":item.last_modified_i,
                      "has_fulltext":item.has_fulltext,"first_publish_year":item.first_publish_year} for item in result]
        
        for data in data_list:
            print(data)

        return data_list

    except Exception as e:

        print(f"An error has occured {str(e)}")
        raise HTTPException(status_code=500,detail="Internal Server Error")









