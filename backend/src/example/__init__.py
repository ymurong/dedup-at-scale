from .controller import example_app
from src.common.load_data import load_data
from src.example.database import DATABASE_URL

# import data
load_data(DATABASE_URL)
