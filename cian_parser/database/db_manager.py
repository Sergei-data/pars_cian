from sqlalchemy import create_engine
import pandas as pd
from pathlib import Path
import logging

logger = logging.getLogger(__name__)

class DatabaseManager:
    
    def __init__(self, dbname="cian_db", user="postgres", password="12345", host="localhost", port=5432):
        self.engine = create_engine(f"postgresql+psycopg2://{user}:{password}@{host}:{port}/{dbname}")
        logger.info("Подключение к БД установлено")

    def append_city_data(self, csv_path: Path, table_name: str = None):
        if not table_name:
            city = csv_path.stem.replace('data_', '').lower()
            table_name = f"cian_{city}"

        df = pd.read_csv(csv_path)
        df.to_sql(
            name=table_name,
            con=self.engine,
            if_exists='append',
            index=False
        )
        logger.info(f"Добавлено {len(df)} строк в {table_name}")

    def append_all_cities(self, data_dir: str = "data"):
        for city_file in Path(data_dir).glob("data_*.csv"):
            try:
                self.append_city_data(city_file)
            except Exception as e:
                logger.error(f"Ошибка {city_file.name}: {e}")
                continue

    def close(self):
        if self.engine:
            self.engine.dispose()
            logger.info("Соединение закрыто")

