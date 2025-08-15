from minio import Minio
import os

# Initialize client
client = Minio(
    "localhost:9000",
    access_key="minioadmin",
    secret_key="minioadmin",
    secure=False
)

# Upload file
client.fput_object(
    "my-project-bucket",
    "processed_data.csv",
    os.path.join("outputs", "processed_data.csv")
)
print("File uploaded successfully!")