import gdown
from io import BytesIO
from airflow.providers.amazon.aws.hooks.s3 import S3Hook

def get_data2_from_gdrive(file_id, s3_key, s3_bucket, conn_id):

    try:

        print("starting transfer") 

        
        url = f"https://drive.google.com/uc?id={file_id}" 
        

        buffer = BytesIO() 
        
        gdown.download(url, buffer, quiet=False)
        buffer.seek(0)
        
        S3Hook(aws_conn_id=conn_id).load_file_obj(
            file_obj=buffer,
            key=s3_key,
            bucket_name=s3_bucket,
            replace=True
        )
        print(f"Success: {s3_key} uploaded to {s3_bucket}")
        return True
    except Exception as e:
        print(f"Failed: {str(e)}")
        return False

if __name__ == "__main__":
        
        get_data2_from_gdrive(file_id="188tIKLJKek62rGmzj1Ylc03fe4Pgb5co",
                              s3_key = "raw/item_movie_lens.csv", 
                              s3_bucket= "dec-kate--lightat2003",
                              conn_id = "s3_conn") 