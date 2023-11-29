import os
from sqlalchemy import create_engine, Column, Integer, String, Boolean, DateTime, func , MetaData , Table
from sqlalchemy.orm import sessionmaker
from sqlalchemy.ext.declarative import declarative_base
from datetime import datetime
import time
import ijson
import logging
from sqlalchemy.exc import IntegrityError

logging.basicConfig(filename='data_pipeline.log', level=logging.INFO, format='%(asctime)s %(levelname)s %(message)s')

# Folder where JSON files are located
json_folder = '/Users/ameena/Documents/DP-DataEngineer-tests'
# PostgreSQL connection string
db_url = "postgresql+pg8000://Datapipeline_DB_user:Datapipeline_DB_pwd@0.0.0.0:5432/Datapipeline_DB"
engine = create_engine(db_url)
Base = declarative_base()


# Create the database engine
class BookData(Base):
    __tablename__ = 'book_data_new'

    id = Column(Integer, primary_key=True, autoincrement=True)
    key = Column(String, nullable=False)
    title = Column(String)
    last_modified_i = Column(DateTime)
    has_fulltext = Column(Boolean)
    first_publish_year = Column(Integer)

try:
    Base.metadata.create_all(engine)
    print("Table created successfully!")
except Exception as e:
    print(f"Error creating the table: {str(e)}")

Session = sessionmaker(autocommit=False, autoflush=False, bind=engine)

# Create the metadata_timestamp tbl
def create_metadata_table(Base):
    metadata_obj = MetaData()

    class MetadataTimestamp(Base):
        __tablename__ = 'metadata_timestamp_new'

        id = Column('id', Integer, primary_key=True)
        last_read_timestamp = Column(DateTime(timezone=True), server_default=func.now())

    metadata_obj.reflect(bind=engine)

    if 'metadata_timestamp_new' not in metadata_obj.tables:
        print("Creating metadata_timestamp_new table")
        MetadataTimestamp.__table__.create(bind=engine)
    else:
        MetadataTimestamp.__table__ = metadata_obj.tables['metadata_timestamp_new']

    return MetadataTimestamp.__table__

def extract_transform_load(json_data):
    try:
        transformed_data = {
            "key": json_data.get("key", ""),
            "title": json_data.get("title_suggest", ""),
            "last_modified_i": datetime.fromtimestamp(json_data.get("last_modified_i", 0)),
            "has_fulltext": bool(json_data.get("has_fulltext", False)),
            "first_publish_year": int(json_data.get("first_publish_year", 0))
        }
        print("transformed data is :", transformed_data)
        return transformed_data
    except Exception as e:
        print(f"Error processing JSON data: {e}")
        return None

def process_json_files(session , metadata_timestamp):

    #json_file_list = list(filter(lambda x : x.endswith('.json'), os.listdir(json_folder)))
    json_file_list = [
        file for file in os.listdir(json_folder) if file.endswith('.json')
        and os.path.getmtime(os.path.join(json_folder, file)) > metadata_timestamp.last_read_timestamp.timestamp()
    ]   

    for filename in json_file_list:
        if filename.endswith('.json'):
            file_path = os.path.join(json_folder, filename)
            logging.info("Processing file:", file_path)
            books = []  # List to store book data
            book_data = {}
            with open(file_path, 'r') as f:
                parser = ijson.parse(f)

                for prefix, event, value in parser:
                    if prefix.startswith('books.item'):
                        if event == 'start_map':
                            book_data = {}  # Start of a new book entry
                        elif event == 'map_key':
                            key = value  # The current key within the book entry
                        elif event in ['string', 'number', 'boolean', 'null']:
                            book_data[key] = value  # Assign value to the current key
                        elif event == 'end_map':
                            transformed_data = extract_transform_load(book_data)
                            books.append(transformed_data)
   
                for item in books:
                    logging.info(f"Inserting {books} books to postgres")

                    try:
                
                        insert_query = BookData.__table__.insert().values(
                            key=item['key'],
                            title=item['title'],
                            last_modified_i=item['last_modified_i'],
                            has_fulltext=item['has_fulltext'],
                            first_publish_year=item['first_publish_year']
                        )
                        session.execute(insert_query)
                        session.commit()
                    except IntegrityError as e:
                        session.rollback()
                        logging.error(f"IntegrityError: {str(e)}")
                        # Handle the duplicate key error here

if __name__ == "__main__":

    Base.metadata.create_all(engine) 
    metadata_timestamp = create_metadata_table(Base)

    while True:
        try:
            session = Session(bind=engine)  # Create a session within the loop
            timestamp_values = session.query(metadata_timestamp.last_read_timestamp).all()
            timestamps = [value[0] for value in timestamp_values]

            process_json_files(session, timestamps)
            print("Waiting for 30 minutes before checking the folder again...")
            session.execute(metadata_timestamp.update().values(last_read_timestamp=datetime.now()))
            session.commit()
            session.close()  # Close the session
            time.sleep(1800)
        except Exception as e:
            logging.error(f"Error: {str(e)}")
