# FireFeed - AI-powered newsfeed

Новостной агрегатор с поддержкой AI

Официальный сайт - https://firefeed.net

## Описание проекта

FireFeed - это современный новостной агрегатор с поддержкой искусственного интеллекта, разработанный для автоматического сбора, обработки и распространения новостей на нескольких языках.

### Основные возможности

#### AI-powered обработка контента
- **Автоматический перевод новостей** на 4 языка (русский, немецкий, французский, английский) с использованием современных моделей машинного обучения (Helsinki-NLP OPUS-MT, M2M100)
- **Обнаружение дубликатов** с помощью семантического анализа и векторных эмбеддингов (Sentence Transformers)
- **Интеллектуальная обработка изображений** с автоматическим извлечением и оптимизацией

#### Многоязычная поддержка
- Полностью локализованный Telegram-бот с поддержкой 4 языков
- REST API с многоязычным интерфейсом
- Адаптивная система переводов с учетом терминологии

#### Гибкая система RSS
- **Автоматический парсинг** более 50 RSS-лент различных источников
- **Категоризация новостей** по темам (мировые новости, технологии, спорт, экономика и др.)
- **Персонализированные подписки** пользователей на категории и источники
- **Пользовательские RSS-ленты** - возможность добавления собственных источников

#### Безопасная архитектура
- **JWT-аутентификация** для API
- **Шифрование паролей** с использованием bcrypt
- **Валидация email** с кодом подтверждения
- **Защищенное хранение секретов** через переменные окружения (.env)

#### Высокая производительность
- **Асинхронная архитектура** на базе asyncio
- **Пул соединений PostgreSQL** для эффективной работы с БД
- **Очереди задач** для параллельной обработки переводов
- **Кэширование моделей** ML для оптимизации памяти

#### Технический стек

**Backend:**
- Python 3.8+ с asyncio
- FastAPI для REST API
- PostgreSQL с pgvector для семантического поиска
- aiopg для асинхронных запросов к БД

**AI/ML:**
- Transformers (Hugging Face)
- Sentence Transformers для эмбеддингов
- SpaCy для обработки текста
- Torch для вычислений

**Интеграции:**
- Telegram Bot API
- SMTP для email-уведомлений
- Webhook-поддержка

**Инфраструктура:**
- Docker-контейнеризация
- systemd для управления сервисами
- nginx для проксирования

#### Архитектура

Проект состоит из нескольких ключевых компонентов:

1. **Telegram Bot** (`bot.py`) - основной интерфейс взаимодействия с пользователями
2. **RSS Parser Service** (`rss_parser.py`) - фоновая служба парсинга RSS-лент
3. **REST API** (`api/main.py`) - веб-API для внешних интеграций
4. **Translation Engine** (`firefeed_translator.py`) - система переводов с кэшированием
5. **Duplicate Detector** (`firefeed_dublicate_detector.py`) - обнаружение дубликатов через ML
6. **User Management** (`user_manager.py`) - управление пользователями и подписками

### Масштабируемость и надежность

- **Горизонтальное масштабирование** через микросервисную архитектуру
- **Отказоустойчивость** с автоматическими перезапусками и логированием
- **Мониторинг производительности** с подробной телеметрией
- **Graceful shutdown** для корректного завершения работы

## Установка зависимостей перед запуском

```bash
pip install -r requirements.txt
```

## Запуск

### Запуск через команды

```bash
python -m venv venv
source venv/bin/activate
python bot.py
```

### Запуск через bash-скрипт

```bash
chmod +x ./run_bot.sh
./run_bot.sh
```

### Запуск Telegram-бота и FastAPI через systemd-юниты

Пример файла сервиса Telegram-бота:

```bash
[Unit]
Description=FireFeed Telegram Bot Service
After=network.target

[Service]
Type=simple
User=firefeed
Group=firefeed
WorkingDirectory=/var/www/firefeed/data/integrations/telegram

ExecStart=/var/www/firefeed/data/integrations/telegram/run_bot.sh

Restart=on-failure
RestartSec=10

TimeoutStopSec=30
KillMode=mixed
KillSignal=SIGTERM
SendSIGKILL=yes

[Install]
WantedBy=multi-user.target
```

Пример файла сервиса API:

```bash
[Unit]
Description=Firefeed News API (FastAPI)
After=network.target
After=postgresql@17-main.service
Wants=postgresql@17-main.service

[Service]
Type=simple
User=firefeed
Group=firefeed

WorkingDirectory=/var/www/firefeed/data/integrations/telegram
ExecStart=/var/www/firefeed/data/integrations/telegram/run_api.sh

Restart=always
RestartSec=5

StandardOutput=journal
StandardError=journal

[Install]
WantedBy=multi-user.target
```

Пример конфигурации nginx для работы через webhook и FastAPI:

```lua
upstream fastapi_app {
    server 127.0.0.1:8000; # Если Uvicorn запущен с --host 127.0.0.1
    # server unix:/path/to/your/uvicorn.sock; # Если используете Unix-сокет (более производительно) }
}

server {
    ...

    location /webhook {
        proxy_pass http://127.0.0.1:5000/webhook;
        proxy_set_header Host $host;
        proxy_set_header X-Real-IP $remote_addr;
        proxy_set_header X-Forwarded-For $proxy_add_x_forwarded_for;
    }

    location /api/ {
        proxy_pass http://fastapi_app;

        proxy_set_header Host $http_host;
        proxy_set_header X-Real-IP $remote_addr;
        proxy_set_header X-Forwarded-For $proxy_add_x_forwarded_for;
        proxy_set_header X-Forwarded-Proto $scheme;

        proxy_connect_timeout 60s;
        proxy_send_timeout 60s;
        proxy_read_timeout 60s;

        # Для поддержки WebSockets (если используете в FastAPI)
        proxy_http_version 1.1;
        proxy_set_header Upgrade $http_upgrade;
        proxy_set_header Connection "upgrade";
    }
}

```
