import asyncio
import sys
import os

# Добавляем корень проекта в путь поиска модулей
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from email_service.sender import send_verification_email

async def test_email():
    # Замени на свой email для тестирования
    test_email = "yurem@bk.ru"  # <-- Замени на реальный email
    verification_code = "123456"
    
    print(f"Отправляем тестовое письмо на {test_email}")
    print(f"Код подтверждения: {verification_code}")
    
    # Тестируем отправку на разных языках
    for language in ['en', 'ru', 'de']:
        print(f"\nТестируем отправку на языке: {language}")
        success = send_verification_email(test_email, verification_code, language)
        if success:
            print(f"✅ Письмо на {language} успешно отправлено!")
        else:
            print(f"❌ Ошибка при отправке письма на {language}")
    
    print("\nТест завершен!")

if __name__ == "__main__":
    asyncio.run(test_email())