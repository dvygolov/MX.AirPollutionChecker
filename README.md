# MX.AirPollutionChecker

Бот для мессенджера **МАХ**, который мониторит качество воздуха в районе Волгарь (г.Самара) и отправляет уведомления подписчикам при превышении ПДК.

## Откуда данные

Источник: [https://pogoda-sv.ru/pollcenter/volgar/](https://pogoda-sv.ru/pollcenter/volgar/)  
Это сайт ФГБУ «Приволжское УГМС» — автоматический пост №91 (Софийская площадь).

### API-эндпоинты (открытые, без авторизации)

```
GET https://pogoda-sv.ru/pollcenter/airdata/api/get_station_meas_last_list?station=11
GET https://pogoda-sv.ru/pollcenter/airdata/api/get_station_comment_last_list?station=11
```

Ответ включает:
- `meas_list` — метаданные о веществах (название, единицы, ПДК, класс опасности)
- `meas_last_list` — последние измерения (значение, дата, `factor` — доля от ПДК)
- `status` — общий уровень опасности (0 — норма)

Превышение ПДК определяется по полю `factor`: `factor > 1.0` значит превышение.

## Архитектура

```
               +------------------+
               | Приволжское |
               |      УГМС     |
               +--------+---------+
                        | GET /api/get_station_meas_last_list
                        v
+----------+     +------+------+     +---------+
| Пользователи | <-> | Бот (Python) | <-> |   МАХ    |
|  МАХ     |     |  httpx + SQLite |     | API     |
+----------+     +-----------------+     +---------+
```

## Команды бота

| Команда | Описание |
|----------|----------|
| `/start` | Подписаться на уведомления |
| `/stop` | Отписаться |
| `/status` | Текущее качество воздуха |
| `/help` | Справка |

## Запуск

1. **Установить зависимости**

```bash
pip install httpx python-dotenv
# или
uv add httpx python-dotenv
```

2. **Скопировать `.env.example` в `.env` и заполнить**

```bash
cp .env.example .env
```

3. **Запустить**

```bash
python bot.py
```

## Переменные окружения

| Переменная | Описание | По умолчанию |
|------------|----------|----------|
| `MAX_BOT_TOKEN` | Bearer-токен бота МАХ | **обязательно** |
| `MAX_TARGET_CHAT_ID` | chat_id группы/канала МАХ | **обязательно** |
| `MAX_API_BASE` | База API МАХ | `https://platform-api.max.ru` |
| `FACTOR_THRESHOLD` | Порог превышения ПДК | `1.0` |
| `POLL_INTERVAL_SEC` | Интервал проверки (с) | `300` |
| `WATCHED_MEAS_IDS` | ID веществ через запятую | `пусто = все` |
| `STATE_DB_PATH` | Путь к SQLite | `bot_state.db` |
| `LOG_LEVEL` | Уровень логирования | `INFO` |

## Совместимость с существующим ботом МАХ

Этот бот пользуется тем же API МАХ, что и `TG.MaxSyncBot`:
- База: `MAX_API_BASE` (default `https://platform-api.max.ru`)
- Авторизация: `Authorization: Bearer <token>`
- Отправка сообщений: `POST /messages?chat_id=...`
- Получение: `GET /messages?chat_id=...&count=...`
