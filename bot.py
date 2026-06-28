"""
Бот для мессенджера МАХ, мониторит качество воздуха в Волгаре (Самара).
Использует библиотеку maxapi (https://github.com/love-apples/maxapi).
"""

from __future__ import annotations

import asyncio
import logging
import os
import sqlite3
from dataclasses import dataclass, field
from datetime import datetime, timezone
from threading import Lock
from typing import Any

import httpx
from dotenv import load_dotenv
from maxapi import Bot, Dispatcher, F
from maxapi.filters.command import CommandStart
from maxapi.types import BotStarted, MessageCreated


@dataclass
class Settings:
    max_bot_token: str
    state_db_path: str
    poll_interval_sec: int
    log_level: str
    factor_threshold: float
    watched_meas_ids: list[int] = field(default_factory=list)


def load_settings() -> Settings:
    load_dotenv()
    def required(name: str) -> str:
        value = os.getenv(name)
        if not value:
            raise RuntimeError(f"Missing required environment variable: {name}")
        return value
    def env_int(name: str, default: int, min_value: int | None = None) -> int:
        raw = os.getenv(name)
        if raw is None:
            return default
        try:
            value = int(raw.strip())
        except Exception:
            return default
        if min_value is not None and value < min_value:
            return min_value
        return value
    def env_float(name: str, default: float) -> float:
        raw = os.getenv(name)
        if raw is None:
            return default
        try:
            return float(raw.strip())
        except Exception:
            return default
    def env_list_int(name: str, default: list[int]) -> list[int]:
        raw = os.getenv(name)
        if not raw:
            return default
        try:
            return [int(x.strip()) for x in raw.split(",") if x.strip()]
        except Exception:
            return default
    return Settings(
        max_bot_token=required("MAX_BOT_TOKEN"),
        state_db_path=os.getenv("STATE_DB_PATH", "bot_state.db"),
        poll_interval_sec=env_int("POLL_INTERVAL_SEC", 300, min_value=30),
        log_level=os.getenv("LOG_LEVEL", "INFO"),
        factor_threshold=env_float("FACTOR_THRESHOLD", 1.0),
        watched_meas_ids=env_list_int("WATCHED_MEAS_IDS", []),
    )


class StateStore:
    def __init__(self, path: str) -> None:
        self.path = path
        self.lock = Lock()
        self.conn = sqlite3.connect(path)
        self._ensure_tables()

    def _ensure_tables(self) -> None:
        with self.lock:
            self.conn.execute("CREATE TABLE IF NOT EXISTS subscribers (chat_id INTEGER PRIMARY KEY)")
            self.conn.execute("""
                CREATE TABLE IF NOT EXISTS bot_state (
                    key TEXT PRIMARY KEY,
                    value TEXT NOT NULL,
                    updated_at INTEGER NOT NULL DEFAULT (strftime('%s','now'))
                )
            """)
            self.conn.execute("""
                CREATE TABLE IF NOT EXISTS alert_history (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    alert_hash TEXT NOT NULL UNIQUE,
                    meas_id INTEGER,
                    factor REAL,
                    value_convert REAL,
                    fullname TEXT,
                    created_at INTEGER NOT NULL DEFAULT (strftime('%s','now'))
                )
            """)
            self.conn.commit()

    def close(self) -> None:
        with self.lock:
            self.conn.close()

    def add_subscriber(self, chat_id: int) -> bool:
        with self.lock:
            try:
                self.conn.execute("INSERT INTO subscribers (chat_id) VALUES (?)", (chat_id,))
                self.conn.commit()
                return True
            except sqlite3.IntegrityError:
                return False

    def remove_subscriber(self, chat_id: int) -> bool:
        with self.lock:
            cur = self.conn.execute("DELETE FROM subscribers WHERE chat_id = ?", (chat_id,))
            self.conn.commit()
            return cur.rowcount > 0

    def get_subscribers(self) -> list[int]:
        with self.lock:
            rows = self.conn.execute("SELECT chat_id FROM subscribers ORDER BY chat_id").fetchall()
        return [row[0] for row in rows]

    def subscriber_count(self) -> int:
        with self.lock:
            row = self.conn.execute("SELECT COUNT(*) FROM subscribers").fetchone()
        return row[0] if row else 0

    def set_state(self, key: str, value: str) -> None:
        with self.lock:
            self.conn.execute("""
                INSERT INTO bot_state (key, value) VALUES (?, ?)
                ON CONFLICT(key) DO UPDATE SET value=excluded.value,
                    updated_at=strftime('%s','now')
            """, (key, value))
            self.conn.commit()

    def get_state(self, key: str) -> str | None:
        with self.lock:
            row = self.conn.execute("SELECT value FROM bot_state WHERE key = ?", (key,)).fetchone()
        return row[0] if row else None

    def was_alert_sent(self, alert_hash: str) -> bool:
        with self.lock:
            row = self.conn.execute("SELECT 1 FROM alert_history WHERE alert_hash = ?", (alert_hash,)).fetchone()
        return row is not None

    def record_alert(self, alert_hash: str, meas_id: int, factor: float, value_convert: float, fullname: str) -> None:
        with self.lock:
            self.conn.execute("""
                INSERT OR IGNORE INTO alert_history (alert_hash, meas_id, factor, value_convert, fullname)
                VALUES (?, ?, ?, ?, ?)
            """, (alert_hash, meas_id, factor, value_convert, fullname))
            self.conn.commit()


POLL_API_URL = "https://pogoda-sv.ru/pollcenter/airdata/api/get_station_meas_last_list"
COMMENT_API_URL = "https://pogoda-sv.ru/pollcenter/airdata/api/get_station_comment_last_list"


class AirDataClient:
    def __init__(self) -> None:
        self.http = httpx.AsyncClient(
            timeout=httpx.Timeout(30.0, connect=10.0),
            headers={
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/126.0.0.0 Safari/537.36",
                "Accept": "application/json",
            },
        )

    async def close(self) -> None:
        await self.http.aclose()

    async def fetch_latest(self, station: int = 11) -> dict[str, Any]:
        resp = await self.http.get(POLL_API_URL, params={"station": station})
        resp.raise_for_status()
        data = resp.json()
        station_data = data.get(str(station), {})
        return {
            "meas_list": station_data.get("meas_list", {}),
            "meas_last_list": station_data.get("meas_last_list", {}),
            "status": station_data.get("status", {}),
        }

    async def fetch_comment(self, station: int = 11) -> str:
        resp = await self.http.get(COMMENT_API_URL, params={"station": station})
        resp.raise_for_status()
        data = resp.json()
        comment = data.get(str(station), {}).get("comment_last", {})
        return comment.get("value", "") if isinstance(comment, dict) else ""


class AirPollutionBot:
    def __init__(self, settings: Settings) -> None:
        self.settings = settings
        self.store = StateStore(settings.state_db_path)
        self.air = AirDataClient()

    async def close(self) -> None:
        await self.air.close()
        self.store.close()

    async def handle_start(self, chat_id: int) -> str:
        added = self.store.add_subscriber(chat_id)
        count = self.store.subscriber_count()
        if added:
            return (
                f"<b>✅ Вы подписаны!</b>\n\n"
                "Бот мониторит качество воздуха в районе Волгарь "
                "(г.Самара, автопост №91) и пришлёт "
                "<b>уведомление лично вам</b>, если "
                "превышение ПДК будет зафиксировано.\n\n"
                f"👥 Подписчиков: <b>{count}</b>\n"
                f"📊 Порог алерта: <b>{self.settings.factor_threshold}×ПДК</b>\n"
                f"⏰ Интервал проверки: <b>{self.settings.poll_interval_sec // 60} мин</b>\n\n"
                "Отписаться: /stop\n"
                "Текущий статус: /status"
            )
        return "ℹ️ Вы уже подписаны.\n\nОтписаться: /stop\nТекущий статус: /status"

    async def handle_stop(self, chat_id: int) -> str:
        removed = self.store.remove_subscriber(chat_id)
        if removed:
            return "❌ <b>Вы отписаны</b> от уведомлений о качестве воздуха."
        return "ℹ️ Вы не были подписаны."

    async def handle_status(self) -> str:
        try:
            data = await self.air.fetch_latest()
            comment = await self.air.fetch_comment()
            status = data["status"]
            meas_list = data["meas_list"]
            last_list = data["meas_last_list"]
            begin_local = status.get("max_begin_at_local_formatted", "—")

            lines = [
                "<b>🌍 Качество воздуха — Волгарь, Самара</b>",
                "<i>Автопост №91, Софийская площадь</i>",
                f"<i>Срок наблюдения: {begin_local}</i>",
                "",
            ]

            poll_items = [(m.get("ordering", 0), mid, m, last_list.get(mid, {}))
                          for mid, m in meas_list.items() if m.get("type") == "poll"]
            poll_items.sort(key=lambda x: x[0])

            any_exceed = False
            for _, mid, meta, last in poll_items:
                factor = last.get("factor")
                value = last.get("value_convert")
                limit = meta.get("concentration_limit")
                name = meta.get("fullname", mid)

                if factor is None:
                    lines.append(f"⚪ <b>{name}</b> — нет данных")
                    continue

                pct = factor * 100
                if factor == 0:
                    emoji = "⚪"
                elif factor < 0.5:
                    emoji = "🟢"
                elif factor < 1.0:
                    emoji = "🟡"
                elif factor < 2.0:
                    emoji = "🟠"
                    any_exceed = True
                else:
                    emoji = "🔴"
                    any_exceed = True

                warning = " ⚠️" if factor > self.settings.factor_threshold else ""
                lines.append(f"{emoji} <b>{name}</b>{warning}\n└ <code>{value} / {limit} мг/м³</code> — <b>{pct:.1f}%</b> ПДК")

            meteo_parts = []
            for mid, meta in meas_list.items():
                if meta.get("type") != "meteorological":
                    continue
                last = last_list.get(mid, {})
                val = last.get("value_convert", "—")
                fname = meta.get("fullname", "")
                if "Температура" in fname:
                    meteo_parts.append(f"🌡 <b>{val}°C</b>")
                elif "Скорость ветра" in fname:
                    meteo_parts.append(f"💨 <b>{val} м/с</b>")
                elif "Направление" in fname:
                    meteo_parts.append(f"🧭 <b>{val}</b>")

            if meteo_parts:
                lines.append("")
                lines.append("<blockquote>" + " │ ".join(meteo_parts) + "</blockquote>")

            if comment:
                clean = comment.replace("<!--", "").replace("-->", "").strip()
                clean = clean.replace("<br>", "\n").replace("<br/>", "\n")
                if clean:
                    lines.append("")
                    lines.append(f"<i>💬 {clean}</i>")

            lines.append("")
            lines.append(f"ℹ️ Порог алерта: <b>{self.settings.factor_threshold}×ПДК</b>")
            if any_exceed:
                lines.append(
                    "🚨 <b>Внимание!</b> Зафиксировано превышение ПДК.\n"
                    "👥 Подпишитесь: /start"
                )

            text = "\n".join(lines)
            if len(text) > 4000:
                text = text[:3950] + "\n\n<i>...сообщение обрезано</i>"
            return text
        except Exception:
            logging.exception("Failed to build status")
            return "⚠️ Не удалось получить данные. Попробуйте позже."

    async def handle_help(self) -> str:
        return (
            "<b>🛡 Команды бота</b>\n\n"
            "<code>/start</code> — подписаться на уведомления\n"
            "<code>/stop</code> — отписаться\n"
            "<code>/status</code> — текущее качество воздуха с индикаторами\n"
            "<code>/help</code> — эта справка\n\n"
            "<b>О мониторинге</b>\n"
            "Данные с автоматического поста "
            "№91 (Софийская площадь, р-н Волгарь) "
            "от ФГБУ «Приволжское УГМС». "
            f"Бот проверяет каждые "
            f"{self.settings.poll_interval_sec // 60} минут "
            f"и шлёт алерт при пороге "
            f"<b>{self.settings.factor_threshold}×ПДК</b>."
        )

    async def check_and_alert(self, bot: Bot) -> list[dict[str, Any]]:
        data = await self.air.fetch_latest()
        status = data["status"]
        meas_list = data["meas_list"]
        last_list = data["meas_last_list"]

        current_begin_at = status.get("max_begin_at")
        last_begin_at = self.store.get_state("last_begin_at")
        if current_begin_at == last_begin_at:
            return []
        if current_begin_at:
            self.store.set_state("last_begin_at", current_begin_at)

        alerts: list[dict[str, Any]] = []
        for meas_id_str, meta in meas_list.items():
            if meta.get("type") != "poll":
                continue
            meas_id = int(meas_id_str)
            if self.settings.watched_meas_ids and meas_id not in self.settings.watched_meas_ids:
                continue
            last = last_list.get(meas_id_str, {})
            factor = last.get("factor")
            if factor is None or factor <= self.settings.factor_threshold:
                continue

            alert = await self._send_alert(meas_id, meta, last, status, bot)
            if alert:
                alerts.append(alert)
        return alerts

    async def _send_alert(self, meas_id: int, meta: dict, last: dict, status: dict, bot: Bot) -> dict[str, Any] | None:
        factor = last["factor"]
        value = last.get("value_convert", "—")
        limit = meta.get("concentration_limit", "—")
        name = meta.get("fullname", str(meas_id))
        begin_at = status.get("max_begin_at_local_formatted", "—")
        alert_hash = f"{begin_at}|{meas_id}|{factor:.2f}"

        if self.store.was_alert_sent(alert_hash):
            return None

        pct = factor * 100
        emoji = "🟣" if factor >= 5.0 else ("🔴" if factor >= 2.0 else "🟠")
        text = (
            f"<b>{emoji} ПРЕВЫШЕНИЕ ПДК!</b>\n\n"
            f"<b>{name}</b>\n"
            f"└ <code>{value} / {limit} мг/м³</code>\n"
            f"<b>{pct:.1f}%</b> ПДК ({factor:.2f}×)\n\n"
            f"<i>Срок: {begin_at}</i>\n"
            "<i>Пост №91, Софийская площадь, р-н Волгарь</i>"
        )

        subscribers = self.store.get_subscribers()
        if not subscribers:
            logging.warning("Alert detected but no subscribers")
            self.store.record_alert(alert_hash, meas_id, factor, value, name)
            return None

        sent = 0
        for chat_id in subscribers:
            try:
                await bot.send_message(chat_id=chat_id, text=text, format="html")
                sent += 1
                await asyncio.sleep(0.25)
            except Exception:
                logging.exception("Alert send failed to chat_id=%s", chat_id)

        self.store.record_alert(alert_hash, meas_id, factor, value, name)
        logging.warning("ALERT: %s (%.2f×) sent to %d/%d", name, factor, sent, len(subscribers))
        return {
            "meas_id": meas_id,
            "fullname": name,
            "factor": factor,
            "subscribers_total": len(subscribers),
            "subscribers_reached": sent,
        }


async def _monitor_loop(bot: AirPollutionBot, max_bot: Bot, settings: Settings) -> None:
    while True:
        try:
            alerts = await bot.check_and_alert(max_bot)
            for a in alerts:
                logging.info("Alert: %s (%.2f×ПДК) reached %d/%d", a["fullname"], a["factor"], a["subscribers_reached"], a["subscribers_total"])
        except Exception:
            logging.exception("Monitor loop error")
        await asyncio.sleep(settings.poll_interval_sec)


async def main() -> None:
    settings = load_settings()
    logging.basicConfig(level=settings.log_level, format="%(asctime)s %(levelname)s %(message)s")

    max_bot = Bot(token=settings.max_bot_token)
    app = AirPollutionBot(settings)
    dp = Dispatcher()

    @dp.bot_started()
    async def on_bot_started(event: BotStarted) -> None:
        app.store.add_subscriber(event.chat_id)
        reply = await app.handle_start(event.chat_id)
        await max_bot.send_message(chat_id=event.chat_id, text=reply, format="html")

    @dp.message_created(CommandStart())
    async def on_start(event: MessageCreated) -> None:
        chat_id = event.message.recipient.chat_id
        if chat_id is None:
            chat_id = event.message.recipient.user_id
        app.store.add_subscriber(chat_id)
        reply = await app.handle_start(chat_id)
        await event.message.answer(text=reply, format="html")

    @dp.message_created(F.text.lower().in_({"/stop", "отписаться", "stop"}))
    async def on_stop(event: MessageCreated) -> None:
        chat_id = event.message.recipient.chat_id
        if chat_id is None:
            chat_id = event.message.recipient.user_id
        reply = await app.handle_stop(chat_id)
        await event.message.answer(text=reply, format="html")

    @dp.message_created(F.text.lower().in_({"/status", "статус", "status"}))
    async def on_status(event: MessageCreated) -> None:
        reply = await app.handle_status()
        await event.message.answer(text=reply, format="html")

    @dp.message_created(F.text.lower().in_({"/help", "помощь", "help"}))
    async def on_help(event: MessageCreated) -> None:
        reply = await app.handle_help()
        await event.message.answer(text=reply, format="html")

    try:
        await asyncio.gather(
            dp.start_polling(max_bot),
            _monitor_loop(app, max_bot, settings),
        )
    finally:
        await app.close()


if __name__ == "__main__":
    asyncio.run(main())
