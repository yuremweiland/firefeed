#!/usr/bin/env python3
"""
Модуль для периодической очистки базы данных.

Включает функции для удаления неверифицированных и удаленных пользователей.
"""

import asyncio
import logging
from datetime import datetime, timedelta

import config
from api import database

logger = logging.getLogger(__name__)


async def cleanup_users():
    """Удаляет неверифицированных и удаленных пользователей."""
    pool = await database.get_db_pool()
    if pool is None:
        logger.error("Не удалось получить пул подключений к БД")
        return

    try:
        async with pool.acquire() as conn:
            async with conn.cursor() as cur:
                # Удаляем пользователей с is_deleted = TRUE
                await cur.execute("DELETE FROM users WHERE is_deleted = TRUE")
                deleted_count = cur.rowcount
                logger.info(f"Удалено {deleted_count} удаленных пользователей")

                # Удаляем неверифицированных пользователей, зарегистрированных более 24 часов назад
                cutoff_time = datetime.utcnow() - timedelta(hours=24)
                await cur.execute(
                    "DELETE FROM users WHERE is_verified = FALSE AND created_at < %s",
                    (cutoff_time,)
                )
                unverified_count = cur.rowcount
                logger.info(f"Удалено {unverified_count} неверифицированных пользователей (старше 24 часов)")

                total_deleted = deleted_count + unverified_count
                logger.info(f"Всего удалено пользователей: {total_deleted}")

    except Exception as e:
        logger.error(f"Ошибка при очистке пользователей: {e}")
    finally:
        await database.close_db_pool()


async def periodic_cleanup_users():
    """Периодическая задача очистки пользователей (раз в 24 часа)."""
    while True:
        await asyncio.sleep(24 * 60 * 60)  # 24 часа
        try:
            await cleanup_users()
        except Exception as e:
            logger.error(f"Ошибка в периодической очистке пользователей: {e}")