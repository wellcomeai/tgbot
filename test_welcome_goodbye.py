import asyncio
import os
import sys
from telegram import Bot, InlineKeyboardButton, InlineKeyboardMarkup
from telegram import ReplyKeyboardMarkup, KeyboardButton, ReplyKeyboardRemove
from database import Database

BOT_TOKEN = os.environ.get('BOT_TOKEN')
CHANNEL_ID = os.environ.get('CHANNEL_ID')
TEST_USER_ID = 8045097843

def personalize_message(text: str, username: str = "voicyfyAI_support", first_name: str = "Voicyfy", last_name: str = "") -> str:
    """Персонализация как в main.py"""
    username_value = f"@{username}" if username else first_name
    text = text.replace('{username}', username_value)
    text = text.replace('{first_name}', first_name)
    text = text.replace('{last_name}', last_name)
    return text

async def test_welcome_and_goodbye():
    """
    Комплексный тест приветственного и прощального сообщений
    """
    
    print(f"\n{'='*70}")
    print(f"🧪 КОМПЛЕКСНЫЙ ТЕСТ: ПРИВЕТСТВИЕ + ПРОЩАНИЕ")
    print(f"   User: {TEST_USER_ID}")
    print(f"   Channel: {CHANNEL_ID}")
    print(f"{'='*70}\n")
    
    bot = Bot(token=BOT_TOKEN)
    db = Database()
    
    # ============================================
    # ЧАСТЬ 1: ПРОВЕРКА СТАТУСА БОТА В КАНАЛЕ
    # ============================================
    print("📊 ПРОВЕРКА СТАТУСА БОТА В КАНАЛЕ")
    print("="*70)
    
    try:
        me = await bot.get_me()
        print(f"🤖 Бот: @{me.username} (ID: {me.id})")
        
        # Получаем инфо о канале
        try:
            chat = await bot.get_chat(CHANNEL_ID)
            print(f"📢 Канал: {chat.title} (ID: {CHANNEL_ID})")
            print(f"   Тип: {chat.type}")
        except Exception as e:
            print(f"❌ Канал не найден: {e}")
            return
        
        # Проверяем статус бота
        try:
            member = await bot.get_chat_member(CHANNEL_ID, me.id)
            print(f"\n✅ Статус бота в канале: {member.status}")
            
            if member.status == 'administrator':
                print(f"🎉 Бот - АДМИНИСТРАТОР")
                print(f"   Права:")
                print(f"   • Может удалять сообщения: {member.can_delete_messages}")
                print(f"   • Может ограничивать пользователей: {member.can_restrict_members}")
                print(f"   • Может приглашать: {member.can_invite_users}")
                admin_rights_ok = True
            elif member.status == 'member':
                print(f"⚠️ Бот - ОБЫЧНЫЙ УЧАСТНИК (не админ)")
                print(f"   ❌ БЕЗ ПРАВ АДМИНА НЕТ СОБЫТИЙ chat_member!")
                admin_rights_ok = False
            else:
                print(f"❓ Статус: {member.status}")
                admin_rights_ok = False
                
        except Exception as e:
            print(f"❌ Ошибка проверки прав: {e}")
            print(f"   Бот НЕ добавлен в канал или НЕ админ")
            admin_rights_ok = False
            
    except Exception as e:
        print(f"❌ КРИТИЧЕСКАЯ ОШИБКА: {e}")
        return
    
    # ============================================
    # ЧАСТЬ 2: ТЕСТ ПРИВЕТСТВЕННОГО СООБЩЕНИЯ
    # ============================================
    print(f"\n\n{'='*70}")
    print("👋 ТЕСТ ПРИВЕТСТВЕННОГО СООБЩЕНИЯ")
    print("="*70)
    
    try:
        welcome_data = db.get_welcome_message()
        welcome_buttons = db.get_welcome_buttons()
        
        print(f"📝 Настройки приветствия:")
        print(f"   Текст: {welcome_data['text'][:60]}...")
        print(f"   Фото: {'✅ Есть' if welcome_data['photo'] else '❌ Нет'}")
        print(f"   Механических кнопок: {len(welcome_buttons)}")
        
        if welcome_buttons:
            for i, (btn_id, btn_text, pos) in enumerate(welcome_buttons, 1):
                print(f"      {i}. '{btn_text}'")
        
        # Персонализируем текст
        personalized_text = personalize_message(
            welcome_data['text'],
            "voicyfyAI_support",
            "Voicyfy",
            ""
        )
        
        # Создаем клавиатуру
        if welcome_buttons:
            keyboard = [[KeyboardButton(btn_text)] for _, btn_text, _ in welcome_buttons]
            reply_markup = ReplyKeyboardMarkup(
                keyboard,
                resize_keyboard=True,
                one_time_keyboard=True
            )
        else:
            keyboard = [
                [KeyboardButton("✅ Согласиться на получение уведомлений")],
                [KeyboardButton("📋 Что я буду получать?")],
                [KeyboardButton("ℹ️ Подробнее о боте")]
            ]
            reply_markup = ReplyKeyboardMarkup(keyboard, resize_keyboard=True)
        
        print(f"\n📤 Отправка приветственного сообщения...")
        
        # Отправляем
        if welcome_data['photo']:
            sent = await bot.send_photo(
                chat_id=TEST_USER_ID,
                photo=welcome_data['photo'],
                caption=personalized_text,
                parse_mode='HTML',
                reply_markup=reply_markup
            )
            print(f"✅ Приветствие с ФОТО отправлено! (msg_id: {sent.message_id})")
        else:
            sent = await bot.send_message(
                chat_id=TEST_USER_ID,
                text=personalized_text,
                parse_mode='HTML',
                reply_markup=reply_markup
            )
            print(f"✅ Приветствие ТЕКСТ отправлено! (msg_id: {sent.message_id})")
        
        print(f"💬 Проверь Telegram - должно прийти приветствие с кнопками!")
        
    except Exception as e:
        print(f"❌ ОШИБКА отправки приветствия: {e}")
        import traceback
        traceback.print_exc()
    
    # Пауза
    await asyncio.sleep(2)
    
    # ============================================
    # ЧАСТЬ 3: ТЕСТ ПРОЩАЛЬНОГО СООБЩЕНИЯ
    # ============================================
    print(f"\n\n{'='*70}")
    print("😢 ТЕСТ ПРОЩАЛЬНОГО СООБЩЕНИЯ")
    print("="*70)
    
    try:
        goodbye_data = db.get_goodbye_message()
        goodbye_buttons = db.get_goodbye_buttons()
        
        print(f"📝 Настройки прощания:")
        print(f"   Текст: {goodbye_data['text'][:60]}...")
        print(f"   Фото: {'✅ Есть' if goodbye_data['photo'] else '❌ Нет'}")
        print(f"   Inline кнопок: {len(goodbye_buttons)}")
        
        if goodbye_buttons:
            for i, (btn_id, btn_text, btn_url, pos) in enumerate(goodbye_buttons, 1):
                print(f"      {i}. '{btn_text}' → {btn_url}")
        
        # Персонализируем текст
        personalized_text = personalize_message(
            goodbye_data['text'],
            "voicyfyAI_support",
            "Voicyfy",
            ""
        )
        
        # Создаем inline клавиатуру
        reply_markup = None
        if goodbye_buttons:
            keyboard = [[InlineKeyboardButton(btn_text, url=btn_url)] 
                       for _, btn_text, btn_url, _ in goodbye_buttons]
            reply_markup = InlineKeyboardMarkup(keyboard)
        
        print(f"\n📤 Отправка прощального сообщения...")
        
        # Отправляем
        if goodbye_data['photo']:
            sent = await bot.send_photo(
                chat_id=TEST_USER_ID,
                photo=goodbye_data['photo'],
                caption=personalized_text,
                parse_mode='HTML',
                reply_markup=reply_markup
            )
            print(f"✅ Прощание с ФОТО отправлено! (msg_id: {sent.message_id})")
        else:
            sent = await bot.send_message(
                chat_id=TEST_USER_ID,
                text=personalized_text,
                parse_mode='HTML',
                reply_markup=reply_markup
            )
            print(f"✅ Прощание ТЕКСТ отправлено! (msg_id: {sent.message_id})")
        
        print(f"💬 Проверь Telegram - должно прийти прощание!")
        
        # Убираем клавиатуру после прощания
        await asyncio.sleep(1)
        await bot.send_message(
            chat_id=TEST_USER_ID,
            text="⬆️ Это было прощальное сообщение (для теста)",
            reply_markup=ReplyKeyboardRemove()
        )
        
    except Exception as e:
        print(f"❌ ОШИБКА отправки прощания: {e}")
        import traceback
        traceback.print_exc()
    
    # ============================================
    # ЧАСТЬ 4: ИТОГОВАЯ ДИАГНОСТИКА
    # ============================================
    print(f"\n\n{'='*70}")
    print("🔍 ДИАГНОСТИКА ПРОБЛЕМЫ")
    print("="*70)
    
    print(f"\n1️⃣ Приветственное сообщение:")
    print(f"   ✅ Работает через ChatJoinRequestHandler")
    print(f"   ✅ НЕ требует прав администратора")
    print(f"   ✅ Срабатывает при одобрении join request")
    
    print(f"\n2️⃣ Прощальное сообщение:")
    if admin_rights_ok:
        print(f"   ✅ Бот является администратором")
        print(f"   ✅ ДОЛЖНО работать при выходе пользователя")
        print(f"   ℹ️ Тестируй: пусть тестовый юзер покинет канал")
    else:
        print(f"   ❌ Бот НЕ администратор канала")
        print(f"   ❌ Telegram НЕ шлет события chat_member")
        print(f"   ❌ handle_member_update НЕ вызывается")
        print(f"   🔧 РЕШЕНИЕ: Сделай бота администратором!")
    
    print(f"\n3️⃣ Как исправить:")
    print(f"   1. Открой канал {CHANNEL_ID}")
    print(f"   2. Администраторы → Добавить администратора")
    print(f"   3. Найди @{me.username}")
    print(f"   4. Выдай права:")
    print(f"      ✅ Управление пользователями")
    print(f"      ✅ Удаление сообщений")
    print(f"   5. Проверь: запусти этот тест снова")
    
    print(f"\n{'='*70}")
    print(f"✅ ТЕСТ ЗАВЕРШЕН")
    print(f"{'='*70}\n")

if __name__ == "__main__":
    if not BOT_TOKEN or not CHANNEL_ID:
        print("❌ BOT_TOKEN или CHANNEL_ID не установлены!")
        sys.exit(1)
    
    asyncio.run(test_welcome_and_goodbye())
