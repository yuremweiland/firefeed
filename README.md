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

### Запуск через systemd-юнит

Пример файла сервиса:

```bash
[Unit]
Description=FireFeed Telegram Bot Service
After=network.target

[Service]
Type=simple
User=firefeed
Group=firefeed
WorkingDirectory=/var/www/firefeed/data/www/firefeed.net/integrations/telegram


ExecStartPre=/bin/sh -c 'pids=$(lsof -t -i:5000); [ -n "$pids" ] && kill -9 $pids || true'
ExecStart=/var/www/firefeed/data/www/firefeed.net/integrations/telegram/run_bot.sh
ExecStopPost=/bin/sh -c 'pids=$(lsof -t -i:5000); [ -n "$pids" ] && kill -9 $pids || true'

Restart=on-failure
RestartSec=10

TimeoutStopSec=5
KillMode=process
KillSignal=SIGINT

[Install]
WantedBy=multi-user.target
```

Пример конфигурации nginx для работы через webhook:

```lua
    location /webhook {
        proxy_pass http://127.0.0.1:5000/webhook;
        proxy_set_header Host $host;
        proxy_set_header X-Real-IP $remote_addr;
        proxy_set_header X-Forwarded-For $proxy_add_x_forwarded_for;
    }
```