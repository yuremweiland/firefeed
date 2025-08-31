# FireFeed - AI-powered newsfeed

Новостной агрегатор с поддержкой AI

Официальный сайт - https://firefeed.net

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


ExecStartPre=/bin/sh -c 'pids=$(lsof -t -i:5000); [ -n "$pids" ] && kill -9 $pids || true'
ExecStart=/var/www/firefeed/data/integrations/telegram/run_bot.sh
ExecStopPost=/bin/sh -c 'pids=$(lsof -t -i:5000); [ -n "$pids" ] && kill -9 $pids || true'

Restart=on-failure
RestartSec=10

TimeoutStopSec=5
KillMode=process
KillSignal=SIGINT

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

## Troubleshooting

1. async/await псина
2. Если запускаем бота не от пользователя firefeed, то не работают переводы - т.к. нет доступа к nltk словарям