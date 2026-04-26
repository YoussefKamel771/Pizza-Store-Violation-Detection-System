from pydantic_settings import BaseSettings , SettingsConfigDict
from typing import List

class Settings(BaseSettings):

    APP_NAME: str
    APP_VERSION: str

    conn_str: str 
    roi_config_path: str
    test_video_path: str = "../test_data/Sah w b3dha ghalt (3).mp4"
    model_path: str = "../weights/best.pt"
    

    # model_config = SettingsConfigDict(env_file=".env")
    class Config:
        env_file = "../.env"

def get_settings():
    return Settings()