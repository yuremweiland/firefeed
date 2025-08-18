#!/bin/bash

# Активируем виртуальное окружение
source /var/www/firefeed/data/www/firefeed.net/integrations/telegram/venv/bin/activate

# Запускаем бота
python /var/www/firefeed/data/www/firefeed.net/integrations/telegram/bot.py
