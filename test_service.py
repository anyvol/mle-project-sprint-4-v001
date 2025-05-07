# Тестирование микросервиса рекомендаций

import time
import random
import requests
import pandas as pd

recommendations_url = "http://127.0.0.1:8000"
features_store_url = "http://127.0.0.1:8010"
events_store_url = "http://127.0.0.1:8020"
headers = {'Content-type': 'application/json', 'Accept': 'text/plain'}

# 1. Для пользователя без персональных рекомендаций
print("\n\nТЕСТ 1")
print(f'Выбираем пользователя без персональных рекомендаций...')
personal_recs_users = set(pd.read_parquet("recommendations.parquet")['user_id'])
all_users = set(pd.read_parquet("events.parquet")["user_id"])
cold_users = list(all_users.difference(personal_recs_users))
random_cold_user = random.choice(cold_users)
print(f"Выбранный холодный пользователь: {random_cold_user}")

k = 10
params = {"user_id": random_cold_user, 'k': k}

resp = requests.post(recommendations_url + "/recommendations", headers=headers, params=params)
if resp.status_code == 200:
    default_recs = resp.json()
else:
    default_recs = []
    print(f"status code: {resp.status_code}")
print(f"{k} рекомендаций по умолчанию для пользователя {random_cold_user}:\n{default_recs}")
time.sleep(3)


# 2. Для пользователя с персональными рекомендациями, но без онлайн-истории,
print("\n\nТЕСТ 2")
print("Выбираем пользователя для персональных рекомендаций...")
random_user_for_personal = random.choice(list(personal_recs_users))
print(f'Для персональных рекомендаций выбран пользователь №{random_user_for_personal}')
k = 10
params = {"user_id": random_user_for_personal, 'k': k}

resp = requests.post(recommendations_url + "/recommendations_offline", headers=headers, params=params)
if resp.status_code == 200:
    offline_recs = resp.json()
else:
    offline_recs = []
    print(f"status code: {resp.status_code}")
    
print(f"{k} персональных рекомендаций для пользователя {random_user_for_personal}:\n{offline_recs}") 
time.sleep(3)

# 3. Для пользователя с персональными рекомендациями и онлайн-историей.

print("\n\nТЕСТ 3")
print(f'Добавляем события прослушивания для пользователей... (подробности в ноутбуке тестирования)')
event_item_ids =  [43202169, 27077792, 93556487, 109302]

for event_item_id in event_item_ids:
    resp = requests.post(events_store_url + "/put", 
                        headers=headers, 
                        params={"user_id": random_user_for_personal, "item_id": event_item_id})
print(f"Добавлены прослушивания пользователем №{random_user_for_personal} треков {event_item_ids}")

params = {"user_id": random_user_for_personal, 'k': 3} # k - количество подходящих на одно взаимодействие
resp = requests.post(recommendations_url + "/recommendations_online", headers=headers, params=params)
online_recs = resp.json()
print(f'Полученные онлайн рекомендации для пользователя: {online_recs}')


print("Запросим у сервиса для пользователя смешанные онлайн и оффлайн (чередуются онлайн, затем оффлайн) рекомендации")
k = 10
params = {"user_id": random_user_for_personal, 'k': k}

resp = requests.post(recommendations_url + "/recommendations", headers=headers, params=params)
if resp.status_code == 200:
    recs = resp.json()
else:
    recs = []
    print(f"status code: {resp.status_code}")
print(f"Смешанные оффлайн и онлайн рекомендации для пользователя {random_user_for_personal}: {recs}")