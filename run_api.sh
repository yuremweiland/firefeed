#!/bin/bash

# Убедимся, что pyenv загружен
export PYENV_ROOT="$HOME/.pyenv"
export PATH="$PYENV_ROOT/bin:$PATH"
eval "$(pyenv init -)"

# Устанавливаем версию Python
pyenv shell 3.13.6

# Запускаем FastAPI через uvicorn
uvicorn api.main:app --host 127.0.0.1 --port 8000
