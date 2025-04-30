import logging
from contextlib import asynccontextmanager

import pandas as pd
from fastapi import FastAPI

logger = logging.getLogger("uvicorn.error")
# берём похожее из локального файла, а не из S3, чтобы быстрее загружалось
SIMILAR_ITEMS_FILENAME = 'similar.parquet'

class SimilarItems:
    """
    Класс для работы с похожими треками
    """
    def __init__(self):

        self._similar_items = None

    def load(self, path, **kwargs):
        """
        Загружаем данные из файла
        """

        logger.info(f"Загрузка похожих треков стартовала")
        self._similar_items = pd.read_parquet(path, **kwargs)
        self._similar_items = self._similar_items.set_index("item_id_1")
        logger.info(f"Похожие треки загружены")

    def get(self, item_id: int, k: int = 10):
        """
        Возвращает список похожих объектов
        """
        try:
            i2i = self._similar_items.loc[item_id].head(k)
            i2i = i2i[["item_id_2", "score"]].to_dict(orient="list")
        except KeyError:
            logger.error("Похожие треки не найдены")
            i2i = {"item_id_2": [], "score": {}}

        return i2i

sim_items_store = SimilarItems()

@asynccontextmanager
async def lifespan(app: FastAPI):
    # код ниже (до yield) выполнится только один раз при запуске сервиса
    sim_items_store.load(
        SIMILAR_ITEMS_FILENAME,
        columns=["item_id_1", "item_id_2", "score"],
    )
    logger.info("Сервис с похожими треками готов к работе!")
    # код ниже выполнится только один раз при остановке сервиса
    yield

# создаём приложение FastAPI
app = FastAPI(title="features", lifespan=lifespan)

@app.post("/similar_items")
async def recommendations(item_id: int, k: int = 10):
    """
    Возвращает список похожих треков длиной k для указанного item_id
    """

    i2i = sim_items_store.get(item_id, k)

    return i2i