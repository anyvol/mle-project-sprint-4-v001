import logging
import pandas as pd
import requests
from fastapi import FastAPI
from contextlib import asynccontextmanager

# конфигурируем сохранение логов в файл test_service.log
logger = logging.getLogger("test_service_logs")
stream_handler = logging.StreamHandler()
stream_handler.setLevel(logging.INFO)
logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s %(levelname)s %(module)s, %(funcName)s, %(message)s',
                    handlers=[
                            logging.FileHandler("test_service.log", mode='a'),
                            stream_handler
                            ])

features_store_url = "http://127.0.0.1:8010"
events_store_url = "http://127.0.0.1:8020"

# берём рекомендации из локальных файлов, а не из S3, чтобы быстрее загружалось
PERSONAL_RECS_FILENAME = 'recommendations.parquet' 
POPULAR_RECS_FILENAME = 'top_popular.parquet'

class Recommendations:
    """
    Класс для работы с рекомендациями
    """
    
    def __init__(self):

        self._recs = {"personal": None, "default": None}
        self._stats = {
            "request_personal_count": 0,
            "request_default_count": 0,
        }

    def load(self, recommendations_type, path, **kwargs):
        """
        Загружает рекомендации из файла
        """

        logger.info(f"Начинается загрузка {recommendations_type} рекомендации")
        self._recs[recommendations_type] = pd.read_parquet(path, **kwargs)
        if recommendations_type == "personal":
            self._recs[recommendations_type] = self._recs[recommendations_type].set_index("user_id")
        logger.info(f"{recommendations_type} рекомендации загружены")

    def get(self, user_id: int, k: int=100):
        """
        Возвращает список рекомендаций для пользователя
        """
        try:
            recs = self._recs["personal"].loc[user_id]
            recs = recs["item_id"].to_list()[:k]
            self._stats["request_personal_count"] += 1
        except KeyError:
            logger.info(f"Персональные рекомендации для пользователя #{user_id} не найдены. Получаем рекомендации по умолчанию")
            recs = self._recs["default"]
            recs = recs["item_id"].to_list()[:k]
            self._stats["request_default_count"] += 1
        except:
            logger.error(f"Неизвестная ошибка с получением рекомендаций для пользователя #{user_id}")
            recs = []

        return recs

    def stats(self):

        logger.info("Статистика рекомендаций:")
        for name, value in self._stats.items():
            logger.info(f"{name:<30} {value} ")
            


rec_store = Recommendations()

@asynccontextmanager
async def lifespan(app: FastAPI):
    # код ниже (до yield) выполнится только один раз при запуске сервиса
    logger.info("Запуск основного сервиса рекомендаций")

    rec_store.load(
        "personal",
        PERSONAL_RECS_FILENAME,
        columns=["user_id", "item_id", "rank"],
    )
    rec_store.load(
        "default",
        POPULAR_RECS_FILENAME,
        columns=["item_id", "rank"],
    )
    
    yield
    # этот код выполнится только один раз при остановке сервиса
    logger.info("Остановка основного сервиса рекомендаций")
    rec_store.stats()
    
# создаём приложение FastAPI
app = FastAPI(title="recommendations", lifespan=lifespan)

@app.post("/recommendations_offline")
async def recommendations_offline(user_id: int, k: int = 100):
    """
    Возвращает список рекомендаций длиной k для пользователя user_id
    """

    recs = rec_store.get(user_id, k)
    logger.info(f"Получены оффлайн рекомендации для пользователя №{user_id}: {recs}")
    return {"recs": recs}

def dedup_ids(ids):
    """
    Дедублицирует список идентификаторов, оставляя только первое вхождение
    """
    seen = set()
    ids = [id for id in ids if not (id in seen or seen.add(id))]

    return ids

@app.post("/recommendations_online")
async def recommendations_online(user_id: int, k: int = 100):
    """
    Возвращает список онлайн-рекомендаций длиной k для пользователя user_id
    """

    headers = {"Content-type": "application/json", "Accept": "text/plain"}

    # получаем список последних событий пользователя
    params = {"user_id": user_id, "k": 5}
    resp = requests.post(events_store_url + "/get", headers=headers, params=params)
    events = resp.json()["events"]

    # получаем список айтемов, похожих на последние три, с которыми взаимодействовал пользователь
    items = []
    scores = []
    for item_id in events:
        # для каждого item_id получаем список похожих в item_similar_items
        params = {"item_id": item_id, "k": k}
        response = requests.post(features_store_url + "/similar_items", headers=headers, params=params)
        item_similar_items = response.json()

        items += item_similar_items["item_id_2"]
        scores += item_similar_items["score"]
        
    # сортируем похожие объекты по scores в убывающем порядке
    combined = list(zip(items, scores))
    combined = sorted(combined, key=lambda x: x[1], reverse=True)
    combined = [item for item, _ in combined]

    # удаляем дубликаты, чтобы не выдавать одинаковые рекомендации
    recs = dedup_ids(combined)
    logger.info(f"Получены онлайн рекомендации для пользователя №{user_id}: {recs}")
    return {"recs": recs}


@app.post("/recommendations")
async def recommendations(user_id: int, k: int = 100):
    """
    Возвращает список рекомендаций длиной k для пользователя user_id
    """

    recs_offline = await recommendations_offline(user_id, k)
    recs_online = await recommendations_online(user_id, k)

    recs_offline = recs_offline["recs"]
    recs_online = recs_online["recs"]

    recs_blended = []

    min_length = min(len(recs_offline), len(recs_online))
    # чередуем элементы из списков, пока позволяет минимальная длина
    for i in range(min_length):
        recs_blended.append(recs_online[i])
        recs_blended.append(recs_offline[i])

    # добавляем оставшиеся элементы в конец
    recs_blended.extend((recs_offline if len(recs_offline) > len(recs_online) else recs_online)[min_length:])
    # удаляем дубликаты
    recs_blended = dedup_ids(recs_blended)
    
    # оставляем только первые k рекомендаций
    recs_blended = recs_blended[:k] 
    logger.info(f"Получены смешанные онлайн-оффлайн рекомендации для пользователя №{user_id}: {recs_blended}")
    return {"recs": recs_blended}