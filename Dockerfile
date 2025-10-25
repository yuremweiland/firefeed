# Используем официальный образ Python 3.13 slim для уменьшения размера
FROM python:3.13-slim

# Устанавливаем системные зависимости (если нужны для тяжелых библиотек вроде torch)
RUN apt-get update && apt-get install -y \
    build-essential \
    && rm -rf /var/lib/apt/lists/*

# Устанавливаем рабочую директорию
WORKDIR /app

# Копируем requirements.txt и устанавливаем зависимости
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Копируем весь код проекта
COPY . .

# Создаем директорию для данных (если нужно для изображений, но лучше монтировать volume)
RUN mkdir -p /app/data

# Экспортируем порт для API (uvicorn по умолчанию 8000)
EXPOSE 8000

# По умолчанию запускаем API через uvicorn
# Для запуска бота или парсера можно переопределить CMD при запуске контейнера
CMD ["uvicorn", "api.main:app", "--host", "0.0.0.0", "--port", "8000"]