from watchfiles import watch, Change
import pandas as pd
from s3_object_client import AsyncObjectStorage
import asyncio
import logging
from dotenv import load_dotenv
import os

logging.basicConfig(filename='app.log',
                    filemode='a',
                    format='%(asctime)s,%(msecs)03d %(name)s %(levelname)s %(message)s',
                    datefmt='%Y-%m-%d %H:%M:%S',
                    level=logging.INFO)

logging.info("Running pipeline")


load_dotenv()

YOUR_ACCESS_KEY = os.getenv('YOUR_ACCESS_KEY')
YOUR_SECRET_KEY = os.getenv('YOUR_SECRET_KEY')

storage = AsyncObjectStorage(
    key_id= YOUR_ACCESS_KEY,
    secret= YOUR_SECRET_KEY,
    endpoint="https://s3.ru-7.storage.selcloud.ru",
    container='data-engineer-practice-dbatyrshin'
)

"""
Пока добавлена поддержка только для csv файлов
"""

async def remove_file_from_temp(file_path: str):
    os.remove(file_path)

async def send_and_delete_file(file_path: str):
    await storage.send_file(file_path)
    await remove_file_from_temp(file_path)


"""
Pipeline:
"""
filtered_file_index = 0
for changes in watch('files_dir'):
    for change_type, file_path in changes:
        logging.info(f'Изменение типа {change_type} в файле: {file_path}')

        if change_type == Change.added:
            logging.info(f'Create new file: {file_path}')

            if file_path[-3:] != 'csv':
                logging.warning("Unsupported file type")
            else:
                logging.info("Read new cvs file")
                df = pd.read_csv(file_path)
                filtered_df = df[df['product'] == 'Book']
                temp_filepath = f'temporary_dir/filtered{filtered_file_index}.csv'

                logging.info(f"Save filtered dataframe by path {temp_filepath}")
                filtered_df.to_csv(temp_filepath, index=False)

                logging.info(f"Send file to s3")
                asyncio.run(send_and_delete_file(temp_filepath))

                logging.info(f"Delete source file")
                os.remove(file_path)

                logging.info(f"Successful pipeline run")
                filtered_file_index += 1
                logging.info(f"Write logs")
                asyncio.run(storage.send_file("app.log"))