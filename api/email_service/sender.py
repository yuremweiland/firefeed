import smtplib
import ssl
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from typing import Optional
import logging
import os
from jinja2 import Environment, FileSystemLoader
from config import SMTP_CONFIG

# Настройка логирования
logger = logging.getLogger("email_service.sender")
logger.setLevel(logging.INFO)

class EmailSender:
    def __init__(self):
        self.smtp_config = SMTP_CONFIG
        self.sender_email = self.smtp_config['email']
        
        # Настройка Jinja2 для загрузки шаблонов
        template_dir = os.path.join(os.path.dirname(__file__), 'templates')
        self.jinja_env = Environment(loader=FileSystemLoader(template_dir))
        
    def send_verification_email(self, to_email: str, verification_code: str, language: str = 'en') -> bool:
        """
        Отправляет email с кодом подтверждения регистрации
        
        Args:
            to_email (str): Email получателя
            verification_code (str): Код подтверждения
            language (str): Язык письма ('en', 'ru', 'de')
            
        Returns:
            bool: True если письмо отправлено успешно, False в случае ошибки
        """
        try:
            # Создаем сообщение
            message = MIMEMultipart("alternative")
            message["Subject"] = self._get_subject(language)
            message["From"] = self.sender_email
            message["To"] = to_email
            
            # Получаем содержимое письма из шаблонов
            text_content = self._get_text_content(verification_code, language)
            html_content = self._render_html_template(verification_code, language)
            
            # Создаем части письма
            text_part = MIMEText(text_content, "plain", "utf-8")
            html_part = MIMEText(html_content, "html", "utf-8")
            
            # Добавляем части в сообщение
            message.attach(text_part)
            message.attach(html_part)
            
            # Создаем контекст SSL
            context = ssl.create_default_context()
            
            # Отправляем email
            with smtplib.SMTP_SSL(
                self.smtp_config['server'], 
                self.smtp_config['port'], 
                context=context
            ) as server:
                server.login(self.sender_email, self.smtp_config['password'])
                server.sendmail(self.sender_email, to_email, message.as_string())
            
            logger.info(f"Verification email sent successfully to {to_email}")
            return True
            
        except Exception as e:
            logger.error(f"Failed to send verification email to {to_email}: {str(e)}")
            return False
    
    def _get_subject(self, language: str) -> str:
        """Возвращает тему письма в зависимости от языка"""
        subjects = {
            'en': 'FireFeed - Account Verification Code',
            'ru': 'FireFeed - Код подтверждения аккаунта',
            'de': 'FireFeed - Konto-Verifizierungscode'
        }
        return subjects.get(language, subjects['en'])
    
    def _get_text_content(self, verification_code: str, language: str) -> str:
        """Возвращает текстовую версию письма"""
        if language == 'ru':
            return f"""
Добро пожаловать в FireFeed!

Ваш код подтверждения регистрации: {verification_code}

Пожалуйста, введите этот код на странице регистрации для завершения процесса.

С уважением,
Команда FireFeed
            """.strip()
        elif language == 'de':
            return f"""
Willkommen bei FireFeed!

Ihr Konto-Verifizierungscode lautet: {verification_code}

Bitte geben Sie diesen Code auf der Registrierungsseite ein, um den Vorgang abzuschließen.

Mit freundlichen Grüßen,
FireFeed Team
            """.strip()
        else:
            return f"""
Welcome to FireFeed!

Your account verification code is: {verification_code}

Please enter this code on the registration page to complete the process.

Best regards,
FireFeed Team
            """.strip()
    
    def _render_html_template(self, verification_code: str, language: str) -> str:
        """Рендерит HTML шаблон с помощью Jinja2"""
        # Определяем имя файла шаблона
        template_files = {
            'en': 'verification_email_en.html',
            'ru': 'verification_email_ru.html',
            'de': 'verification_email_de.html'
        }
        
        template_name = template_files.get(language, template_files['en'])
        
        try:
            # Загружаем и рендерим шаблон
            template = self.jinja_env.get_template(template_name)
            return template.render(verification_code=verification_code)
        except Exception as e:
            logger.error(f"Failed to render template {template_name}: {str(e)}")
            # Возвращаем базовый HTML контент если шаблон не найден
            return self._get_fallback_html_content(verification_code, language)
    
    def _get_fallback_html_content(self, verification_code: str, language: str) -> str:
        """Возвращает базовый HTML контент если шаблон не найден"""
        if language == 'ru':
            return f"""
<!DOCTYPE html>
<html>
<head>
    <meta charset="UTF-8">
    <title>FireFeed - Подтверждение регистрации</title>
</head>
<body style="font-family: Arial, sans-serif; line-height: 1.6; color: #333;">
    <div style="max-width: 600px; margin: 0 auto; padding: 20px;">
        <div style="text-align: center; margin-bottom: 30px;">
            <h1 style="color: #ff6b35;">🔥 FireFeed</h1>
        </div>
        
        <div style="background-color: #f9f9f9; padding: 30px; border-radius: 10px; border-left: 4px solid #ff6b35;">
            <h2 style="color: #333; margin-top: 0;">Добро пожаловать в FireFeed!</h2>
            
            <p>Спасибо за регистрацию в нашем сервисе новостей.</p>
            
            <div style="background-color: #fff; padding: 20px; border-radius: 5px; text-align: center; margin: 20px 0;">
                <p style="margin: 0; font-size: 16px; color: #666;">Ваш код подтверждения:</p>
                <h3 style="margin: 10px 0; font-size: 32px; color: #ff6b35; letter-spacing: 3px;">{verification_code}</h3>
                <p style="margin: 0; font-size: 14px; color: #999;">Введите этот код на странице регистрации</p>
            </div>
            
            <p>Если вы не регистрировались в FireFeed, просто проигнорируйте это письмо.</p>
        </div>
        
        <div style="text-align: center; margin-top: 30px; color: #999; font-size: 12px;">
            <p>© 2024 FireFeed. Все права защищены.</p>
        </div>
    </div>
</body>
</html>
            """.strip()
        elif language == 'de':
            return f"""
<!DOCTYPE html>
<html>
<head>
    <meta charset="UTF-8">
    <title>FireFeed - Konto-Verifizierung</title>
</head>
<body style="font-family: Arial, sans-serif; line-height: 1.6; color: #333;">
    <div style="max-width: 600px; margin: 0 auto; padding: 20px;">
        <div style="text-align: center; margin-bottom: 30px;">
            <h1 style="color: #ff6b35;">🔥 FireFeed</h1>
        </div>
        
        <div style="background-color: #f9f9f9; padding: 30px; border-radius: 10px; border-left: 4px solid #ff6b35;">
            <h2 style="color: #333; margin-top: 0;">Willkommen bei FireFeed!</h2>
            
            <p>Vielen Dank für Ihre Registrierung bei unserem Nachrichtendienst.</p>
            
            <div style="background-color: #fff; padding: 20px; border-radius: 5px; text-align: center; margin: 20px 0;">
                <p style="margin: 0; font-size: 16px; color: #666;">Ihr Verifizierungscode:</p>
                <h3 style="margin: 10px 0; font-size: 32px; color: #ff6b35; letter-spacing: 3px;">{verification_code}</h3>
                <p style="margin: 0; font-size: 14px; color: #999;">Geben Sie diesen Code auf der Registrierungsseite ein</p>
            </div>
            
            <p>Wenn Sie sich nicht bei FireFeed registriert haben, ignorieren Sie bitte diese E-Mail.</p>
        </div>
        
        <div style="text-align: center; margin-top: 30px; color: #999; font-size: 12px;">
            <p>© 2024 FireFeed. Alle Rechte vorbehalten.</p>
        </div>
    </div>
</body>
</html>
            """.strip()
        else:
            return f"""
<!DOCTYPE html>
<html>
<head>
    <meta charset="UTF-8">
    <title>FireFeed - Account Verification</title>
</head>
<body style="font-family: Arial, sans-serif; line-height: 1.6; color: #333;">
    <div style="max-width: 600px; margin: 0 auto; padding: 20px;">
        <div style="text-align: center; margin-bottom: 30px;">
            <h1 style="color: #ff6b35;">🔥 FireFeed</h1>
        </div>
        
        <div style="background-color: #f9f9f9; padding: 30px; border-radius: 10px; border-left: 4px solid #ff6b35;">
            <h2 style="color: #333; margin-top: 0;">Welcome to FireFeed!</h2>
            
            <p>Thank you for registering with our news service.</p>
            
            <div style="background-color: #fff; padding: 20px; border-radius: 5px; text-align: center; margin: 20px 0;">
                <p style="margin: 0; font-size: 16px; color: #666;">Your verification code:</p>
                <h3 style="margin: 10px 0; font-size: 32px; color: #ff6b35; letter-spacing: 3px;">{verification_code}</h3>
                <p style="margin: 0; font-size: 14px; color: #999;">Enter this code on the registration page</p>
            </div>
            
            <p>If you didn't register with FireFeed, please ignore this email.</p>
        </div>
        
        <div style="text-align: center; margin-top: 30px; color: #999; font-size: 12px;">
            <p>© 2024 FireFeed. All rights reserved.</p>
        </div>
    </div>
</body>
</html>
            """.strip()

# Создаем глобальный экземпляр отправщика
email_sender = EmailSender()

# Удобная функция для отправки письма
def send_verification_email(to_email: str, verification_code: str, language: str = 'en') -> bool:
    """
    Удобная функция для отправки email с кодом подтверждения
    
    Args:
        to_email (str): Email получателя
        verification_code (str): Код подтверждения
        language (str): Язык письма ('en', 'ru', 'de')
        
    Returns:
        bool: True если письмо отправлено успешно, False в случае ошибки
    """
    return email_sender.send_verification_email(to_email, verification_code, language)