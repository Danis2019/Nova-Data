import asyncio  

from pathlib import Path

# Для создания асинхронного контекстного менеджера
from contextlib import asynccontextmanager

# Асинхронная версия boto3
from aiobotocore.session import get_session

# Ошибки при обращении к API
from botocore.exceptions import ClientError, UnknownKeyError


from dotenv import load_dotenv
import os

load_dotenv()


class AsyncObjectStorage:
    def __init__(self, *, key_id: str, secret: str, endpoint: str, container: str):
        self._auth = {
            "aws_access_key_id": key_id,
            "aws_secret_access_key": secret,
            "endpoint_url": endpoint,
            "use_ssl": False,
            "verify": False
        }
        self._bucket = container
        self._session = get_session()

    @asynccontextmanager
    async def _connect(self):
        async with self._session.create_client("s3", **self._auth) as connection:
            yield connection

    async def send_file(self, local_source: str):
        file_ref = Path(local_source)
        target_name = file_ref.name
        async with self._connect() as remote:
            with file_ref.open("rb") as binary_data:
                await remote.put_object(
                    Bucket=self._bucket,
                    Key=target_name,
                    Body=binary_data
                )

    async def fetch_file(self, remote_name: str, local_target: str):
        async with self._connect() as remote:
            response = await remote.get_object(Bucket=self._bucket, Key=remote_name)
            body = await response["Body"].read()
            with open(local_target, "wb") as out:
                out.write(body)

    async def remove_file(self, remote_name: str):
        async with self._connect() as remote:
            await remote.delete_object(Bucket=self._bucket, Key=remote_name)

    async def list_files(self):
        async with self._connect() as remote:
            paginator = remote.get_paginator('list_objects_v2')
            async for result in paginator.paginate(Bucket=self._bucket):
                for c in result.get('Contents', []):
                    print(c)

    async def file_exists(self, remote_name: str):
        async with self._connect() as remote:
            try:
                await remote.get_object(Bucket=self._bucket, Key=remote_name)
                return True
            except ClientError as e:
                if e.response['Error']['Code'] == 'NoSuchKey':
                    return False
                else:
                    raise


YOUR_ACCESS_KEY = os.getenv('YOUR_ACCESS_KEY')
YOUR_SECRET_KEY = os.getenv('YOUR_SECRET_KEY')

async def run_demo():
    storage = AsyncObjectStorage(
        key_id= YOUR_ACCESS_KEY,
        secret= YOUR_SECRET_KEY,
        endpoint="https://s3.ru-7.storage.selcloud.ru",
        container='data-engineer-practice-dbatyrshin'
    )

    print("Список файлов")
    await storage.list_files()

    print("Проверка существования файла")
    result = await storage.file_exists("file.txt")
    print(f"Result file_exists is {result}")


if __name__ == "__main__":
    asyncio.run(run_demo())