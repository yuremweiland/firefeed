#!/bin/bash

# Убедимся, что pyenv загружен
export PYENV_ROOT="$HOME/.pyenv"
export PATH="$PYENV_ROOT/bin:$PATH"
eval "$(pyenv init -)"

# Устанавливаем версию Python
pyenv shell 3.13.6

# Активируем виртуальное окружение
source /var/www/firefeed/data/integrations/telegram/venv/bin/activate

# Запускаем бота
python /var/www/firefeed/data/integrations/telegram/bot.py
