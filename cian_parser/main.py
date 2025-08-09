import os
import logging
from pathlib import Path 

from database.db_manager import DatabaseManager
from parsing.html_parser import array
from data_processing.removing_duplicates import rem_dubl
from data_processing.ref_address import rename_address
from geo_processing.geo_coders import run_geo
from data_processing.ref_coordinates import ref_coor
from cian_parser.geo_processing.distance_to_center import update_datasets_with_distances

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("pipeline.log"),
        logging.StreamHandler()
    ]
)

logger = logging.getLogger(__name__)

class CianPipeline:

    def __init__(self):
        self.checkpoint_file = "checkpoint.txt"
        self.data_dir = "data"
        self._setup_data_dir()

    def _setup_data_dir(self):
        os.makedirs(self.data_dir, exist_ok=True)

    def load_checkpoint(self):
        if os.path.exists(self.checkpoint_file):
            with open(self.checkpoint_file, "r") as f:
                return f.read().strip()
        return ""
    
    def save_checkpoint(self, step):
        with open(self.checkpoint_file, "w") as f:
            f.write(step)
        logger.info(f"Этап '{step}' завершен, чекпоинт сохранен")

    def run_pipeline(self):
        checkpoint = self.load_checkpoint()

        try:
            if not checkpoint or checkpoint == "array":
                logger.info("Этап 1/8: Сбор данных с ЦИАН")
                array()
                self.save_checkpoint("rem_dubl")

            if not checkpoint or checkpoint == "rem_dubl":
                logger.info("Этап 2/8: Удаление дубликатов")
                rem_dubl()
                self.save_checkpoint("rename_address")

            if not checkpoint or checkpoint == "rename_address":
                logger.info("Этап 3/8: Нормализация адресов")
                rename_address()
                self.save_checkpoint("run_geo")

            if not checkpoint or checkpoint == "run_geo":
                logger.info("Этап 4/8: Геокодирование адресов")
                run_geo()
                self.save_checkpoint("ref_coor")

            if not checkpoint or checkpoint == "ref_coor":
                logger.info("Этап 5/8: Форматирование координат")
                ref_coor()
                self.save_checkpoint("center_dist")

            if not checkpoint or checkpoint == "center_dist":
                logger.info("Этап 6/8: Расчет расстояний до центра")
                update_datasets_with_distances()
                self.save_checkpoint("metro_dist")

            if not checkpoint or checkpoint == "db_export":
                logger.info("Этап 7/8: Сохранение данных в PostgreSQL")
                
                db = DatabaseManager()
                try:
                    db.append_all_cities()
                    self.save_checkpoint("cleanup_data") 
                finally:
                    db.close()

            if not checkpoint or checkpoint == "cleanup_data":
                logger.info("Этап 8/8: Очистка папки data")
                self._clean_data_folder()
                self.save_checkpoint("completed")


            if checkpoint == "completed":
                logger.info("Все этапы успешно завершены!")
                os.remove(self.checkpoint_file)

        except Exception as e:
            logger.error(f"Ошибка на этапе '{checkpoint}': {str(e)}", exc_info=True)
            raise

    def _clean_data_folder(self):
        data_path = Path(self.data_dir)
        
        for file in data_path.glob('*'):
            if file.is_file():
                try:
                    file.unlink()
                    logger.info(f"Удален файл: {file.name}")
                except Exception as e:
                    logger.warning(f"Не удалось удалить {file.name}: {e}")


if __name__ == "__main__":
    pipeline = CianPipeline()
    pipeline.run_pipeline()