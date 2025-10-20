import asyncio
import os
import sys
from datetime import datetime
from telegram import Bot, InlineKeyboardButton, InlineKeyboardMarkup
from database import Database

# ============================================
# НАСТРОЙКИ (автоматически из переменных окружения)
# ============================================
BOT_TOKEN = os.environ.get('BOT_TOKEN')      # Токен бота из Render
CHANNEL_ID = os.environ.get('CHANNEL_ID')    # ID канала из Render
TEST_USER_ID = 8045097843                     # ТВОЙ Telegram ID (получит тестовое сообщение)

async def test_goodbye_message():
    """
    Полный тест функционала прощального сообщения:
    1. Проверка БД (текст, фото, кнопки)
    2. Отправка прощального сообщения тебе
    3. Удаление запланированных сообщений
    4. Деактивация пользователя
    5. Проверка прав бота в канале
    6. Реактивация для следующих тестов
    """
    
    print(f"\n{'='*60}")
    print(f"🧪 ТЕСТ ПРОЩАЛЬНОГО СООБЩЕНИЯ")
    print(f"   Тестовый пользователь: {TEST_USER_ID}")
    print(f"   Канал: {CHANNEL_ID}")
    print(f"{'='*60}\n")
    
    # ============================================
    # 1. ПРОВЕРКА БАЗЫ ДАННЫХ
    # ============================================
    print("1️⃣ Проверка базы данных...")
    try:
        db = Database()
        
        # Получаем прощальное сообщение
        goodbye_data = db.get_goodbye_message()
        print(f"   ✅ Текст прощания: {goodbye_data['text'][:70]}...")
        print(f"   ✅ Фото: {goodbye_data['photo'] if goodbye_data['photo'] else '❌ Нет'}")
        
        # Получаем кнопки
        goodbye_buttons = db.get_goodbye_buttons()
        print(f"   ✅ Кнопок настроено: {len(goodbye_buttons)}")
        if goodbye_buttons:
            for i, (btn_id, btn_text, btn_url, pos) in enumerate(goodbye_buttons, 1):
                print(f"      {i}. '{btn_text}' → {btn_url}")
        
        # Проверяем пользователя
        user = db.get_user(TEST_USER_ID)
        if user:
            user_id, username, first_name, joined_at, is_active, bot_started, has_paid, paid_at = user
            print(f"\n   👤 Пользователь {TEST_USER_ID}:")
            print(f"      • Username: @{username}")
            print(f"      • Имя: {first_name}")
            print(f"      • Активен: {'✅ Да' if is_active else '❌ Нет'}")
            print(f"      • Bot started: {'✅ Да' if bot_started else '❌ Нет'}")
            print(f"      • Оплатил: {'✅ Да' if has_paid else '❌ Нет'}")
        else:
            print(f"   ⚠️ ВНИМАНИЕ: Пользователь {TEST_USER_ID} НЕ НАЙДЕН в БД")
            print(f"   Создаем пользователя для теста...")
            db.add_user(TEST_USER_ID, "test_user", "Test User")
            user = db.get_user(TEST_USER_ID)
            print(f"   ✅ Пользователь создан")
        
        # Проверяем запланированные сообщения
        scheduled = db.get_user_scheduled_messages(TEST_USER_ID)
        print(f"\n   📬 Запланированных сообщений: {len(scheduled)}")
        if scheduled and len(scheduled) <= 5:
            for msg_id, msg_num, sch_time, is_sent in scheduled:
                print(f"      • Сообщение #{msg_num} на {sch_time}")
        
    except Exception as e:
        print(f"   ❌ ОШИБКА БД: {e}")
        import traceback
        traceback.print_exc()
        return
    
    # ============================================
    # 2. ТЕСТ ОТПРАВКИ ПРОЩАЛЬНОГО СООБЩЕНИЯ
    # ============================================
    print(f"\n2️⃣ Тест отправки прощального сообщения...")
    bot = Bot(token=BOT_TOKEN)
    
    try:
        # Создаем клавиатуру если есть кнопки
        reply_markup = None
        if goodbye_buttons:
            keyboard = []
            for button_id, button_text, button_url, position in goodbye_buttons:
                keyboard.append([InlineKeyboardButton(button_text, url=button_url)])
            reply_markup = InlineKeyboardMarkup(keyboard)
            print(f"   ✅ Клавиатура создана: {len(goodbye_buttons)} кнопок")
        
        # Отправляем прощальное сообщение
        print(f"   📤 Отправляю прощальное сообщение пользователю {TEST_USER_ID}...")
        
        if goodbye_data['photo']:
            sent = await bot.send_photo(
                chat_id=TEST_USER_ID,
                photo=goodbye_data['photo'],
                caption=goodbye_data['text'],
                parse_mode='HTML',
                reply_markup=reply_markup
            )
            print(f"   ✅ Сообщение с ФОТО отправлено! (message_id: {sent.message_id})")
        else:
            sent = await bot.send_message(
                chat_id=TEST_USER_ID,
                text=goodbye_data['text'],
                parse_mode='HTML',
                reply_markup=reply_markup
            )
            print(f"   ✅ Текстовое сообщение отправлено! (message_id: {sent.message_id})")
        
        print(f"   💬 Проверь свой Telegram - должно прийти прощальное сообщение!")
        
    except Exception as e:
        print(f"   ❌ ОШИБКА ОТПРАВКИ: {e}")
        import traceback
        traceback.print_exc()
        print(f"\n   Возможные причины:")
        print(f"   • Пользователь не начал диалог с ботом (/start)")
        print(f"   • Пользователь заблокировал бота")
        print(f"   • Неверный BOT_TOKEN")
        return
    
    # ============================================
    # 3. ТЕСТ УДАЛЕНИЯ ЗАПЛАНИРОВАННЫХ СООБЩЕНИЙ
    # ============================================
    print(f"\n3️⃣ Тест удаления запланированных сообщений...")
    
    try:
        cancelled = db.cancel_user_messages(TEST_USER_ID)
        print(f"   ✅ Удалено сообщений из БД: {cancelled}")
        
        # Проверяем что действительно удалились
        remaining = db.get_user_scheduled_messages(TEST_USER_ID)
        if len(remaining) == 0:
            print(f"   ✅ Подтверждено: все сообщения удалены из БД")
        else:
            print(f"   ⚠️ ВНИМАНИЕ: Осталось {len(remaining)} сообщений!")
            
    except Exception as e:
        print(f"   ❌ ОШИБКА УДАЛЕНИЯ: {e}")
    
    # ============================================
    # 4. ТЕСТ ДЕАКТИВАЦИИ ПОЛЬЗОВАТЕЛЯ
    # ============================================
    print(f"\n4️⃣ Тест деактивации пользователя...")
    
    try:
        db.deactivate_user(TEST_USER_ID)
        user_after = db.get_user(TEST_USER_ID)
        
        if user_after[4] == 0:  # is_active = 0
            print(f"   ✅ Пользователь успешно деактивирован (is_active = 0)")
        else:
            print(f"   ❌ ОШИБКА: Пользователь все еще активен!")
            
    except Exception as e:
        print(f"   ❌ ОШИБКА ДЕАКТИВАЦИИ: {e}")
    
    # ============================================
    # 5. ПРОВЕРКА ПРАВ БОТА В КАНАЛЕ
    # ============================================
    print(f"\n5️⃣ Проверка прав бота в канале...")
    
    try:
        me = await bot.get_me()
        print(f"   🤖 Бот: @{me.username} (ID: {me.id})")
        
        # Пытаемся получить информацию о боте в канале
        member = await bot.get_chat_member(CHANNEL_ID, me.id)
        
        print(f"   📊 Статус бота в канале: {member.status}")
        
        if member.status == 'administrator':
            print(f"   ✅ Бот является АДМИНИСТРАТОРОМ канала")
            print(f"   ℹ️ Права администратора: {member.can_manage_chat}")
            
            # Проверяем важное право для получения chat_member событий
            if hasattr(member, 'can_restrict_members'):
                print(f"   • Управление пользователями: {'✅' if member.can_restrict_members else '❌'}")
        elif member.status == 'member':
            print(f"   ❌ Бот НЕ является администратором (обычный участник)")
            print(f"   ⚠️ БЕЗ ПРАВ АДМИНА БОТ НЕ ПОЛУЧИТ СОБЫТИЯ ВЫХОДА ПОЛЬЗОВАТЕЛЕЙ!")
        else:
            print(f"   ⚠️ Статус: {member.status}")
            
    except Exception as e:
        print(f"   ❌ ОШИБКА ПРОВЕРКИ ПРАВ: {e}")
        print(f"   Возможные причины:")
        print(f"   • Бот не добавлен в канал")
        print(f"   • Неверный CHANNEL_ID")
    
    # ============================================
    # 6. СИМУЛЯЦИЯ СОБЫТИЯ ВЫХОДА
    # ============================================
    print(f"\n6️⃣ Симуляция события выхода из канала...")
    print(f"   ℹ️ Реальный выход отслеживается через:")
    print(f"   • ChatMemberHandler в main.py")
    print(f"   • Функция handle_member_update()")
    print(f"   ")
    print(f"   📋 Требования для работы:")
    print(f"   ✅ Бот должен быть АДМИНОМ канала {CHANNEL_ID}")
    print(f"   ✅ У бота должно быть право 'Управление пользователями'")
    print(f"   ✅ Webhook должен получать 'chat_member' события")
    print(f"   ✅ allowed_updates должен включать 'chat_member'")
    
    # ============================================
    # 7. РЕАКТИВАЦИЯ ДЛЯ СЛЕДУЮЩИХ ТЕСТОВ
    # ============================================
    print(f"\n7️⃣ Реактивация пользователя для следующих тестов...")
    
    try:
        db.ensure_user_exists_and_active(TEST_USER_ID, "test_user", "Test User")
        user_final = db.get_user(TEST_USER_ID)
        
        if user_final[4] == 1:  # is_active = 1
            print(f"   ✅ Пользователь реактивирован (is_active = 1)")
        else:
            print(f"   ⚠️ Пользователь все еще неактивен")
            
    except Exception as e:
        print(f"   ❌ ОШИБКА РЕАКТИВАЦИИ: {e}")
    
    # ============================================
    # ИТОГИ
    # ============================================
    print(f"\n{'='*60}")
    print(f"✅ ТЕСТ ЗАВЕРШЕН")
    print(f"{'='*60}")
    print(f"\n📝 Выводы:")
    print(f"1. Если сообщение пришло тебе в Telegram → отправка работает ✅")
    print(f"2. Если сообщения удалились из БД → удаление работает ✅")
    print(f"3. Если бот НЕ админ канала → добавь его как администратора!")
    print(f"4. Если события выхода не приходят → проверь allowed_updates\n")

if __name__ == "__main__":
    # Проверка переменных окружения
    if not BOT_TOKEN:
        print("❌ ОШИБКА: BOT_TOKEN не установлен!")
        print("   Убедись что переменная окружения BOT_TOKEN настроена в Render")
        sys.exit(1)
    
    if not CHANNEL_ID:
        print("❌ ОШИБКА: CHANNEL_ID не установлен!")
        print("   Убедись что переменная окружения CHANNEL_ID настроена в Render")
        sys.exit(1)
    
    print(f"✅ BOT_TOKEN: {BOT_TOKEN[:10]}...")
    print(f"✅ CHANNEL_ID: {CHANNEL_ID}")
    print(f"✅ TEST_USER_ID: {TEST_USER_ID}")
    
    # Запускаем тест
    asyncio.run(test_goodbye_message())
