import os
import json
import time
import logging
import asyncio
import threading
import re
import uuid
from threading import Lock
from datetime import datetime
from flask import Flask, session, request, jsonify, render_template, send_from_directory
from flask_socketio import SocketIO, emit, join_room, leave_room

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

app = Flask(__name__)
app.secret_key = os.environ.get("SESSION_SECRET", "telegram_secret_2024")
app.config['PERMANENT_SESSION_LIFETIME'] = 3600 * 24 * 30
app.config['MAX_CONTENT_LENGTH'] = 50 * 1024 * 1024

socketio = SocketIO(
    app,
    cors_allowed_origins="*",
    async_mode='threading',
    ping_timeout=60,
    ping_interval=25,
    logger=False,
    engineio_logger=False,
    allow_upgrades=False,
    transports=['polling']
)

SESSIONS_DIR = "sessions"
UPLOADS_DIR = "static/uploads"
os.makedirs(SESSIONS_DIR, exist_ok=True)
os.makedirs(UPLOADS_DIR, exist_ok=True)

API_ID = 22043994
API_HASH = '56f64582b363d367280db96586b97801'


def parse_entities(raw_text):
    entities = []
    found_raw = set()

    def add(val):
        k = val.lower().lstrip('@')
        if k not in found_raw and len(k) >= 4:
            found_raw.add(k)
            entities.append(val)

    for m in re.findall(r'https?://t\.me/\+([A-Za-z0-9_-]+)', raw_text):
        add(f"+{m}")
    for m in re.findall(r'https?://t\.me/joinchat/([A-Za-z0-9_-]+)', raw_text):
        add(m)
    for m in re.findall(r'https?://t\.me/([A-Za-z][A-Za-z0-9_]{3,})', raw_text):
        add(m)
    for m in re.findall(r'(?<![/\w@])t\.me/\+?([A-Za-z0-9_-]{4,})', raw_text):
        add(m)
    for m in re.findall(r'@([A-Za-z0-9_]{5,})', raw_text):
        add(m)
    for m in re.findall(r'(?<!\d)(-100\d{9,})(?!\d)', raw_text):
        add(m)

    if not entities:
        for part in re.split(r'[\n,،\s|؛;/\\]+', raw_text):
            p = part.strip().lstrip('@')
            if p and len(p) >= 5 and not re.search(r'[أ-ي]', p) and re.match(r'^[A-Za-z0-9_+-]+$', p):
                add(p)

    return entities


def parse_keywords(raw_text):
    seen = set()
    kws = []
    for kw in re.split(r'[\n,،|؛;]+', raw_text):
        kw = kw.strip()
        if kw and kw.lower() not in seen:
            seen.add(kw.lower())
            kws.append(kw)
    return kws


PREDEFINED_USERS = {
    "user_1": {"id": "user_1", "name": "المستخدم الأول", "icon": "fas fa-user", "color": "#5865f2"},
    "user_2": {"id": "user_2", "name": "المستخدم الثاني", "icon": "fas fa-user-tie", "color": "#3ba55c"},
    "user_3": {"id": "user_3", "name": "المستخدم الثالث", "icon": "fas fa-user-graduate", "color": "#faa81a"},
    "user_4": {"id": "user_4", "name": "المستخدم الرابع", "icon": "fas fa-user-cog", "color": "#ed4245"},
    "user_5": {"id": "user_5", "name": "المستخدم الخامس", "icon": "fas fa-user-astronaut", "color": "#6f42c1"},
}

USERS = {}
USERS_LOCK = Lock()


def save_settings(user_id, settings):
    try:
        path = os.path.join(SESSIONS_DIR, f"{user_id}.json")
        with open(path, "w", encoding="utf-8") as f:
            json.dump(settings, f, ensure_ascii=False, indent=2)
        return True
    except Exception as e:
        logger.error(f"Save settings error: {e}")
        return False


def load_settings(user_id):
    try:
        path = os.path.join(SESSIONS_DIR, f"{user_id}.json")
        if os.path.exists(path):
            with open(path, "r", encoding="utf-8") as f:
                return json.load(f)
    except Exception as e:
        logger.error(f"Load settings error: {e}")
    return {}


class UserData:
    def __init__(self, user_id):
        self.user_id = user_id
        self.client_manager = None
        self.settings = {}
        self.stats = {"sent": 0, "errors": 0, "alerts": 0, "replies": 0}
        self.connected = False
        self.authenticated = False
        self.awaiting_code = False
        self.awaiting_password = False
        self.phone_code_hash = None
        self.monitoring_active = False
        self.is_running = False
        self.thread = None
        self.phone_number = None
        self.auto_replies = []
        self.sent_batches = []
        self.telegram_name = None


class TelegramClientManager:
    def __init__(self, user_id):
        self.user_id = user_id
        self.client = None
        self.loop = None
        self.thread = None
        self.stop_flag = threading.Event()
        self.is_ready = threading.Event()
        self.event_handlers_registered = False
        self.scheduled_thread = None
        self.scheduled_stop = threading.Event()
        self.keep_alive_task = None

    def start_client_thread(self):
        if self.thread and self.thread.is_alive():
            return True
        self.stop_flag.clear()
        self.is_ready.clear()
        self.thread = threading.Thread(target=self._run_client_loop, daemon=True)
        self.thread.start()
        return self.is_ready.wait(timeout=45)

    def _run_client_loop(self):
        try:
            from telethon import TelegramClient
            self.loop = asyncio.new_event_loop()
            asyncio.set_event_loop(self.loop)

            session_file = os.path.join(SESSIONS_DIR, f"{self.user_id}_session")
            if API_ID and API_HASH:
                self.client = TelegramClient(session_file, int(API_ID), API_HASH, loop=self.loop)
            else:
                logger.error("API_ID or API_HASH not set")
                self.is_ready.set()
                return

            self.loop.run_until_complete(self._client_main())
        except Exception as e:
            logger.error(f"Client thread error: {e}")
            self.is_ready.set()
        finally:
            if self.loop and not self.loop.is_closed():
                self.loop.close()

    async def _client_main(self):
        retries = 3
        for attempt in range(retries):
            try:
                await self.client.connect()
                self.is_ready.set()
                logger.info(f"✅ {self.user_id} connected (attempt {attempt+1})")

                if await self.client.is_user_authorized():
                    with USERS_LOCK:
                        ud = USERS.get(self.user_id)
                        if ud:
                            ud.authenticated = True
                            ud.connected = True
                            me = await self.client.get_me()
                            name = (me.first_name or '') + (' ' + (me.last_name or '') if me.last_name else '')
                            if not name.strip() and me.username:
                                name = f"@{me.username}"
                            elif not name.strip():
                                name = me.phone or str(me.id)
                            ud.telegram_name = name
                    await self._register_event_handlers()
                    self.keep_alive_task = asyncio.create_task(self._keep_alive())
                    logger.info(f"✅ {self.user_id} auto-authorized")
                    return

                check_counter = 0
                while not self.stop_flag.is_set():
                    await asyncio.sleep(1)
                    check_counter += 1
                    if check_counter >= 30:
                        check_counter = 0
                        with USERS_LOCK:
                            ud = USERS.get(self.user_id)
                            was_auth = ud.authenticated if ud else False
                        if was_auth:
                            try:
                                still_auth = await self.client.is_user_authorized()
                                if not still_auth:
                                    logger.warning(f"⚠️ {self.user_id} session no longer valid")
                                    await self._handle_session_revoked()
                                    break
                            except Exception as check_err:
                                err_str = str(check_err)
                                if any(k in err_str for k in ['AuthKey', 'Unauthorized', 'revoked', 'AUTH_KEY']):
                                    await self._handle_session_revoked()
                                    break
                return

            except Exception as e:
                logger.error(f"Client main error (attempt {attempt+1}/{retries}): {e}")
                if attempt < retries - 1:
                    await asyncio.sleep(3)
                else:
                    self.is_ready.set()
                    raise
            finally:
                if self.client:
                    try:
                        await self.client.disconnect()
                    except:
                        pass

    async def _keep_alive(self):
        while not self.stop_flag.is_set():
            try:
                await self.client.get_me()
                await asyncio.sleep(60)
            except Exception as e:
                logger.warning(f"Keep-alive error for {self.user_id}: {e}")
                await asyncio.sleep(10)

    async def _handle_session_revoked(self):
        logger.info(f"🔴 Session revoked for {self.user_id}")
        with USERS_LOCK:
            ud = USERS.get(self.user_id)
            if ud:
                ud.authenticated = False
                ud.connected = False
                ud.awaiting_code = False
                ud.awaiting_password = False
                ud.monitoring_active = False
                ud.is_running = False
                ud.telegram_name = None

        for suffix in ['_session', '_session.session']:
            path = os.path.join(SESSIONS_DIR, f"{self.user_id}{suffix}")
            if os.path.exists(path):
                try:
                    os.remove(path)
                except:
                    pass

        settings = load_settings(self.user_id)
        settings.pop('phone', None)
        save_settings(self.user_id, settings)

        socketio.emit('session_revoked', {"message": "⚠️ تم إلغاء الجلسة - يرجى تسجيل الدخول مجدداً"}, to=self.user_id)
        socketio.emit('log_update', {"message": "🔴 الجلسة أُلغيت من تيليجرام"}, to=self.user_id)
        self.stop_flag.set()

    async def _start_code_listener(self):
        try:
            from telethon import events
            code_found = asyncio.Event()
            CODE_PATTERN = re.compile(r'\b(\d{5,6})\b')

            def emit_code(code):
                if code_found.is_set():
                    return
                code_found.set()
                socketio.emit('auto_code', {'code': code}, to=self.user_id)
                socketio.emit('log_update', {'message': f'📩 تم استلام كود التحقق ({code}) تلقائياً'}, to=self.user_id)
                logger.info(f"Auto-code for {self.user_id}: {code}")

            @self.client.on(events.NewMessage)
            async def catch_all(event):
                if code_found.is_set():
                    return
                text = event.message.message or ''
                match = CODE_PATTERN.search(text)
                if match:
                    emit_code(match.group(1))
                if event.message.media and hasattr(event.message, 'caption') and event.message.caption:
                    match = CODE_PATTERN.search(event.message.caption)
                    if match:
                        emit_code(match.group(1))

            await asyncio.wait_for(code_found.wait(), timeout=90)

        except asyncio.TimeoutError:
            logger.info(f"Code listener timeout for {self.user_id}")
            socketio.emit('log_update', {'message': '⏰ لم يتم استلام الكود تلقائياً - الرجاء إدخاله يدوياً'}, to=self.user_id)
        except Exception as e:
            logger.error(f"Code listener error: {e}")
        finally:
            try:
                self.client.remove_event_handler(catch_all)
            except:
                pass

    async def _register_event_handlers(self):
        if self.event_handlers_registered:
            return
        try:
            from telethon import events

            @self.client.on(events.NewMessage)
            async def handler(event):
                await self._handle_message(event)

            self.event_handlers_registered = True
            logger.info(f"✅ Event handlers registered for {self.user_id}")
        except Exception as e:
            logger.error(f"Register handlers error: {e}")

    async def _handle_message(self, event):
        try:
            if not event.message.text:
                return

            chat = await event.get_chat()
            chat_title = getattr(chat, 'title', None) or getattr(chat, 'first_name', 'مستخدم')
            chat_username = getattr(chat, 'username', None)
            chat_id = getattr(chat, 'id', None)

            if chat_username:
                group_link = f"https://t.me/{chat_username}"
            elif chat_id:
                group_link = f"https://t.me/c/{str(chat_id).lstrip('-100')}"
            else:
                group_link = None

            with USERS_LOCK:
                ud = USERS.get(self.user_id)
                if not ud:
                    return
                monitoring = ud.monitoring_active
                auto_replies = list(ud.auto_replies or [])
                current_settings = dict(ud.settings)

            msg_text = event.message.text
            msg_lower = msg_text.lower()

            msg_date = event.message.date
            if msg_date:
                try:
                    from datetime import timezone
                    msg_time_str = msg_date.astimezone().strftime('%Y-%m-%d %H:%M:%S')
                except Exception:
                    msg_time_str = msg_date.strftime('%Y-%m-%d %H:%M:%S')
            else:
                msg_time_str = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

            if monitoring:
                watch_words = current_settings.get('watch_words', [])
                if not watch_words:
                    fresh = load_settings(self.user_id)
                    watch_words = fresh.get('watch_words', [])

                msg_normalized = ' '.join(msg_text.split()).lower()

                for kw in watch_words:
                    kw_clean = ' '.join(kw.split()).lower() if kw else ''
                    if kw_clean and (kw_clean in msg_normalized or kw_clean in msg_lower):
                        sender = await event.get_sender()
                        sender_id = getattr(sender, 'id', None)
                        sender_first = getattr(sender, 'first_name', '') or ''
                        sender_last = getattr(sender, 'last_name', '') or ''
                        sender_username = getattr(sender, 'username', None)
                        sender_name = (f"{sender_first} {sender_last}".strip()
                                       or sender_username or str(sender_id) or 'غير معروف')

                        if sender_username:
                            sender_link = f"https://t.me/{sender_username}"
                        elif sender_id:
                            sender_link = f"tg://user?id={sender_id}"
                        else:
                            sender_link = None

                        alert = {
                            "keyword": kw, "group": chat_title, "group_link": group_link,
                            "group_username": chat_username, "group_id": chat_id,
                            "message": msg_text[:500], "full_message": msg_text,
                            "sender": sender_name, "sender_id": sender_id,
                            "sender_username": sender_username, "sender_link": sender_link,
                            "timestamp": datetime.now().strftime('%H:%M:%S'),
                            "message_time": msg_time_str, "message_id": event.message.id
                        }

                        with USERS_LOCK:
                            ud2 = USERS.get(self.user_id)
                            if ud2:
                                ud2.stats['alerts'] = ud2.stats.get('alerts', 0) + 1
                                socketio.emit('stats_update', dict(ud2.stats), to=self.user_id)

                        socketio.emit('new_alert', alert, to=self.user_id)
                        socketio.emit('log_update', {"message": f"🚨 تنبيه: '{kw}' في [{chat_title}] من [{sender_name}]"}, to=self.user_id)

                        try:
                            sender_ref = f"@{sender_username}" if sender_username else sender_name
                            group_ref = f"@{chat_username}" if chat_username else chat_title
                            kw_display = kw[:150] + ('...' if len(kw) > 150 else '')
                            notif = (f"🚨 تنبيه كلمة: {kw_display}\n📍 المجموعة: {group_ref}\n👤 المرسل: {sender_ref}\n⏰ الوقت: {msg_time_str}\n💬 الرسالة:\n{msg_text[:300]}")
                            notif = notif[:4000]
                            await self.client.send_message('me', notif)
                        except Exception:
                            pass

            fresh_rules = load_settings(self.user_id)
            live_auto_replies = fresh_rules.get('auto_replies', auto_replies)
            if not live_auto_replies:
                live_auto_replies = auto_replies

            for rule in live_auto_replies:
                kw = (rule.get('keyword', '') or '').strip()
                reply_text = (rule.get('reply', '') or '').strip()
                kw_clean = ' '.join(kw.split()).lower() if kw else ''
                msg_norm = ' '.join(msg_text.split()).lower()
                if kw_clean and reply_text and (kw_clean in msg_norm or kw_clean in msg_lower):
                    try:
                        sender = await event.get_sender()
                        sender_name = getattr(sender, 'first_name', '') or getattr(sender, 'username', '') or 'مستخدم'

                        MAX_TG = 4096
                        if len(reply_text) <= MAX_TG:
                            await event.message.reply(reply_text)
                        else:
                            await event.message.reply(reply_text[:MAX_TG])
                            for chunk_start in range(MAX_TG, len(reply_text), MAX_TG):
                                await asyncio.sleep(0.5)
                                await self.client.send_message(await event.get_chat(), reply_text[chunk_start:chunk_start+MAX_TG])

                        with USERS_LOCK:
                            ud2 = USERS.get(self.user_id)
                            if ud2:
                                ud2.stats['replies'] = ud2.stats.get('replies', 0) + 1
                                socketio.emit('stats_update', dict(ud2.stats), to=self.user_id)

                        socketio.emit('auto_reply_event', {
                            "sender": sender_name, "chat": chat_title,
                            "original_msg": msg_text[:300], "reply_msg": reply_text[:300],
                            "keyword": kw, "timestamp": datetime.now().strftime('%H:%M:%S')
                        }, to=self.user_id)
                        socketio.emit('log_update', {"message": f"🤖 رد على [{sender_name}] في [{chat_title}] | كلمة: '{kw[:30]}'"}, to=self.user_id)
                    except Exception as e:
                        logger.error(f"Auto-reply send error: {e}")
                        socketio.emit('log_update', {"message": f"❌ فشل الرد التلقائي: {str(e)[:100]}"}, to=self.user_id)
                    break
        except Exception as e:
            logger.error(f"Handle message error: {e}")

    def run_coroutine(self, coro, timeout=30):
        if not self.loop:
            raise Exception("Event loop not initialized")
        future = asyncio.run_coroutine_threadsafe(coro, self.loop)
        return future.result(timeout=timeout)

    def stop(self):
        self.stop_flag.set()
        self.scheduled_stop.set()
        if self.keep_alive_task:
            self.keep_alive_task.cancel()

    def start_scheduled(self, groups, message, image_path, interval_minutes):
        self.scheduled_stop.clear()
        self.scheduled_thread = threading.Thread(
            target=self._scheduled_worker,
            args=(groups, message, image_path, interval_minutes),
            daemon=True
        )
        self.scheduled_thread.start()

    def stop_scheduled(self):
        self.scheduled_stop.set()

    def _scheduled_worker(self, groups, message, image_path, interval_minutes):
        socketio.emit('log_update', {"message": f"📅 بدأ الإرسال المجدول كل {interval_minutes} دقيقة"}, to=self.user_id)
        # إرسال الدفعة الأولى فوراً
        try:
            self.run_coroutine(self._send_to_groups(groups, message, image_path))
        except Exception as e:
            logger.error(f"Scheduled send error (first batch): {e}")
        # ثم التكرار
        while not self.scheduled_stop.is_set():
            self.scheduled_stop.wait(timeout=interval_minutes * 60)
            if not self.scheduled_stop.is_set():
                try:
                    self.run_coroutine(self._send_to_groups(groups, message, image_path))
                except Exception as e:
                    logger.error(f"Scheduled send error: {e}")
        socketio.emit('log_update', {"message": "⏹ تم إيقاف الإرسال المجدول"}, to=self.user_id)

    async def _send_to_groups(self, groups, message, image_path):
        from telethon import functions
        sent = 0
        errors = 0
        total = len(groups)
        batch_id = str(uuid.uuid4())
        has_media = bool(image_path and os.path.exists(image_path))
        batch_entries = []

        socketio.emit('log_update', {"message": f"📤 بدء الإرسال إلى {total} مجموعة..."}, to=self.user_id)

        for i, group in enumerate(groups):
            try:
                entity_str = group.strip()
                chat = None

                if entity_str.startswith('+') and len(entity_str) > 8:
                    try:
                        result = await self.client(functions.messages.ImportChatInviteRequest(hash=entity_str[1:]))
                        chat = result.chats[0] if hasattr(result, 'chats') and result.chats else None
                    except Exception as je:
                        if 'Already' in str(je) or 'USER_ALREADY' in str(je):
                            async for dialog in self.client.iter_dialogs():
                                if hasattr(dialog.entity, 'username') and dialog.entity.username == entity_str:
                                    chat = dialog.entity
                                    break
                        else:
                            raise je
                elif entity_str.lstrip('-').isdigit():
                    chat = await self.client.get_entity(int(entity_str))
                else:
                    username = entity_str.lstrip('@')
                    chat = await self.client.get_entity(f"@{username}")

                if chat is None:
                    raise Exception("لم يتم العثور على المجموعة")

                sent_msg = None
                if has_media:
                    sent_msg = await self.client.send_file(chat, image_path, caption=message or "")
                elif message:
                    sent_msg = await self.client.send_message(chat, message)
                else:
                    raise Exception("لا يوجد محتوى للإرسال")

                sent += 1
                chat_name = getattr(chat, 'title', None) or getattr(chat, 'username', entity_str)
                chat_username = getattr(chat, 'username', None)
                chat_id = getattr(chat, 'id', None)
                msg_id = sent_msg.id if sent_msg else None

                if chat_id and msg_id:
                    batch_entries.append({
                        "chat_id": chat_id, "msg_id": msg_id,
                        "chat_title": chat_name, "chat_username": chat_username,
                        "entity_str": entity_str
                    })

                socketio.emit('log_update', {"message": f"✅ [{i+1}/{total}] أُرسل إلى {chat_name}"}, to=self.user_id)
                with USERS_LOCK:
                    ud = USERS.get(self.user_id)
                    if ud:
                        ud.stats['sent'] = ud.stats.get('sent', 0) + 1
                        socketio.emit('stats_update', dict(ud.stats), to=self.user_id)
                await asyncio.sleep(2)

            except Exception as e:
                errors += 1
                socketio.emit('log_update', {"message": f"❌ [{i+1}/{total}] {group}: {str(e)[:80]}"}, to=self.user_id)
                with USERS_LOCK:
                    ud = USERS.get(self.user_id)
                    if ud:
                        ud.stats['errors'] = ud.stats.get('errors', 0) + 1
                        socketio.emit('stats_update', dict(ud.stats), to=self.user_id)
                await asyncio.sleep(1)

        if batch_entries:
            batch_record = {
                "id": batch_id, "text": message or "", "has_media": has_media,
                "sent_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                "sent_count": sent, "entries": batch_entries
            }
            with USERS_LOCK:
                ud = USERS.get(self.user_id)
                if ud:
                    ud.sent_batches.append(batch_record)
            socketio.emit('batch_saved', batch_record, to=self.user_id)

        socketio.emit('log_update', {"message": f"📊 اكتمل الإرسال: ✅ {sent} ناجح  ❌ {errors} فاشل  من أصل {total}"}, to=self.user_id)
        socketio.emit('send_complete', {"sent": sent, "errors": errors, "total": total}, to=self.user_id)

    async def _edit_batch_messages(self, batch_id, new_text):
        with USERS_LOCK:
            ud = USERS.get(self.user_id)
            if not ud:
                return {"ok": False, "msg": "المستخدم غير موجود"}
            batch = next((b for b in ud.sent_batches if b["id"] == batch_id), None)

        if not batch:
            return {"ok": False, "msg": "الدُّفعة غير موجودة"}

        ok_count = 0
        fail_count = 0
        for entry in batch["entries"]:
            try:
                await self.client.edit_message(entry["chat_id"], entry["msg_id"], new_text)
                ok_count += 1
                socketio.emit('log_update', {"message": f"✏️ تم تعديل الرسالة في {entry['chat_title']}"}, to=self.user_id)
                await asyncio.sleep(0.5)
            except Exception as e:
                fail_count += 1
                socketio.emit('log_update', {"message": f"❌ فشل التعديل في {entry.get('chat_title','?')}: {str(e)[:60]}"}, to=self.user_id)

        with USERS_LOCK:
            ud = USERS.get(self.user_id)
            if ud:
                for b in ud.sent_batches:
                    if b["id"] == batch_id:
                        b["text"] = new_text
                        b["edited_at"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                        break

        socketio.emit('batch_edited', {"batch_id": batch_id, "new_text": new_text, "ok": ok_count, "fail": fail_count}, to=self.user_id)
        return {"ok": True, "edited": ok_count, "failed": fail_count}

    async def _delete_batch_messages(self, batch_id):
        with USERS_LOCK:
            ud = USERS.get(self.user_id)
            if not ud:
                return {"ok": False, "msg": "المستخدم غير موجود"}
            batch = next((b for b in ud.sent_batches if b["id"] == batch_id), None)

        if not batch:
            return {"ok": False, "msg": "الدُّفعة غير موجودة"}

        ok_count = 0
        fail_count = 0
        for entry in batch["entries"]:
            try:
                await self.client.delete_messages(entry["chat_id"], [entry["msg_id"]])
                ok_count += 1
                socketio.emit('log_update', {"message": f"🗑️ تم حذف الرسالة من {entry['chat_title']}"}, to=self.user_id)
                await asyncio.sleep(0.5)
            except Exception as e:
                fail_count += 1
                socketio.emit('log_update', {"message": f"❌ فشل الحذف من {entry.get('chat_title','?')}: {str(e)[:60]}"}, to=self.user_id)

        with USERS_LOCK:
            ud = USERS.get(self.user_id)
            if ud:
                ud.sent_batches = [b for b in ud.sent_batches if b["id"] != batch_id]

        socketio.emit('batch_deleted', {"batch_id": batch_id, "ok": ok_count, "fail": fail_count}, to=self.user_id)
        return {"ok": True, "deleted": ok_count, "failed": fail_count}


def get_or_create_user(user_id):
    with USERS_LOCK:
        if user_id not in USERS:
            ud = UserData(user_id)
            ud.settings = load_settings(user_id)
            ud.auto_replies = ud.settings.get('auto_replies', [])
            if ud.settings.get('phone'):
                ud.phone_number = ud.settings['phone']
            USERS[user_id] = ud
        return USERS[user_id]


def get_current_user_id():
    uid = session.get('user_id', 'user_1')
    if uid not in PREDEFINED_USERS:
        uid = 'user_1'
        session['user_id'] = uid
    return uid


@app.after_request
def add_no_cache(response):
    response.headers['Cache-Control'] = 'no-store, no-cache, must-revalidate, max-age=0'
    response.headers['Pragma'] = 'no-cache'
    response.headers['Expires'] = '0'
    return response


@app.route("/")
def index():
    uid = get_current_user_id()
    get_or_create_user(uid)
    settings = load_settings(uid)
    settings['api_configured'] = bool(API_ID and API_HASH)
    return render_template('index.html',
                           settings=settings,
                           predefined_users=PREDEFINED_USERS,
                           current_user_id=uid)


@app.route("/sw.js")
def service_worker():
    sw_content = """
self.addEventListener('install', event => { self.skipWaiting(); });
self.addEventListener('activate', event => {
    event.waitUntil(caches.keys().then(keys => Promise.all(keys.map(key => caches.delete(key)))));
    self.clients.claim();
});
"""
    from flask import Response
    return Response(sw_content, mimetype='application/javascript')


@app.route("/vite-hmr")
def vite_hmr():
    return "", 204


@app.route("/static/uploads/<path:filename>")
def uploaded_file(filename):
    return send_from_directory(UPLOADS_DIR, filename)


@app.route("/api/get_login_status")
def api_get_login_status():
    uid = get_current_user_id()
    ud = get_or_create_user(uid)
    return jsonify({
        "logged_in": ud.authenticated,
        "connected": ud.connected,
        "awaiting_code": ud.awaiting_code,
        "awaiting_password": ud.awaiting_password,
        "is_running": ud.is_running,
        "phone": ud.phone_number or "",
        "telegram_name": ud.telegram_name if ud.authenticated else None,
        "display_name": ud.telegram_name if ud.authenticated else PREDEFINED_USERS[uid]['name']
    })


@app.route("/api/get_stats")
def api_get_stats():
    uid = get_current_user_id()
    ud = get_or_create_user(uid)
    stats = {
        "sent": ud.stats.get("sent", 0),
        "errors": ud.stats.get("errors", 0),
        "alerts": ud.stats.get("alerts", 0),
        "replies": ud.stats.get("replies", 0),
    }
    return jsonify({"success": True, **stats})


@app.route("/api/parse_input", methods=["POST"])
def api_parse_input():
    data = request.json or {}
    raw = data.get('text', '')
    mode = data.get('mode', 'groups')
    if not raw.strip():
        return jsonify({"success": False, "items": [], "count": 0})
    if mode == 'keywords':
        result = parse_keywords(raw)
    else:
        result = parse_entities(raw)
    return jsonify({"success": True, "items": result, "count": len(result)})


@app.route("/api/get_settings")
def api_get_settings():
    uid = get_current_user_id()
    settings = load_settings(uid)
    return jsonify({"success": True, "settings": settings})


@app.route("/api/get_auto_replies")
def api_get_auto_replies():
    uid = get_current_user_id()
    settings = load_settings(uid)
    return jsonify({"success": True, "auto_replies": settings.get('auto_replies', [])})


@app.route("/api/switch_user", methods=["POST"])
def api_switch_user():
    data = request.json or {}
    new_uid = data.get('user_id')
    if not new_uid or new_uid not in PREDEFINED_USERS:
        return jsonify({"success": False, "message": "مستخدم غير صالح"})
    session['user_id'] = new_uid
    session.permanent = True
    ud = get_or_create_user(new_uid)
    settings = load_settings(new_uid)
    return jsonify({
        "success": True,
        "message": f"✅ تم التبديل إلى {PREDEFINED_USERS[new_uid]['name']}",
        "settings": settings,
        "logged_in": ud.authenticated,
        "awaiting_code": ud.awaiting_code,
        "awaiting_password": ud.awaiting_password,
        "is_running": ud.is_running,
        "telegram_name": ud.telegram_name if ud.authenticated else None,
        "display_name": ud.telegram_name if ud.authenticated else PREDEFINED_USERS[new_uid]['name']
    })


@app.route("/api/save_login", methods=["POST"])
def api_save_login():
    uid = get_current_user_id()
    data = request.json or {}
    phone = data.get('phone', '').strip()

    if not phone:
        return jsonify({"success": False, "message": "❌ أدخل رقم الهاتف"})

    if not API_ID or not API_HASH:
        return jsonify({"success": False, "message": "⚠️ API_ID أو API_HASH غير مضبوط بشكل صحيح"})

    try:
        from telethon.errors import FloodWaitError, RPCError

        ud = get_or_create_user(uid)
        socketio.emit('log_update', {"message": "🔄 جارٍ إعداد الاتصال..."}, to=uid)

        # إعادة تعيين العميل إذا كان غير جاهز
        if ud.client_manager:
            if not ud.client_manager.is_ready.is_set() or ud.client_manager.stop_flag.is_set():
                ud.client_manager.stop()
                ud.client_manager = None

        if not ud.client_manager:
            ud.client_manager = TelegramClientManager(uid)

        if not ud.client_manager.start_client_thread():
            socketio.emit('log_update', {"message": "❌ فشل الاتصال بالخادم - تأكد من اتصال الإنترنت"}, to=uid)
            return jsonify({"success": False, "message": "❌ فشل الاتصال بالخادم. تحقق من اتصال الإنترنت وحاول مجدداً"})

        if not ud.client_manager.client:
            return jsonify({"success": False, "message": "❌ العميل غير جاهز، حاول مرة أخرى"})

        try:
            is_auth = ud.client_manager.run_coroutine(ud.client_manager.client.is_user_authorized(), timeout=15)
        except Exception as auth_err:
            logger.error(f"Auth check error: {auth_err}")
            socketio.emit('log_update', {"message": f"⚠️ خطأ في التحقق: {str(auth_err)[:100]}"}, to=uid)
            return jsonify({"success": False, "message": f"⚠️ خطأ في الاتصال: {str(auth_err)[:100]}"})

        if is_auth:
            try:
                me = ud.client_manager.run_coroutine(ud.client_manager.client.get_me(), timeout=15)
                name = (me.first_name or '') + (' ' + (me.last_name or '') if me.last_name else '')
                if not name.strip() and me.username:
                    name = f"@{me.username}"
                elif not name.strip():
                    name = me.phone or str(me.id)
                with USERS_LOCK:
                    ud.authenticated = True
                    ud.connected = True
                    ud.phone_number = phone
                    ud.telegram_name = name
                settings = load_settings(uid)
                settings['phone'] = phone
                save_settings(uid, settings)
                socketio.emit('log_update', {"message": "✅ تم الدخول تلقائياً (جلسة محفوظة)"}, to=uid)
                return jsonify({"success": True, "message": "✅ أنت مسجل دخول بالفعل", "status": "already_authorized", "telegram_name": name})
            except Exception as me_err:
                logger.error(f"Get me error: {me_err}")
                with USERS_LOCK:
                    ud.authenticated = True
                    ud.connected = True
                    ud.phone_number = phone
                settings = load_settings(uid)
                settings['phone'] = phone
                save_settings(uid, settings)
                socketio.emit('log_update', {"message": "✅ تم الدخول تلقائياً (جلسة محفوظة)"}, to=uid)
                return jsonify({"success": True, "message": "✅ أنت مسجل دخول بالفعل", "status": "already_authorized"})

        socketio.emit('log_update', {"message": f"📱 إرسال كود إلى {phone}..."}, to=uid)
        try:
            sent = ud.client_manager.run_coroutine(ud.client_manager.client.send_code_request(phone), timeout=20)
        except FloodWaitError as e:
            wait_time = e.seconds
            socketio.emit('log_update', {"message": f"⏳ انتظر {wait_time} ثانية ثم حاول مجدداً"}, to=uid)
            return jsonify({"success": False, "message": f"⏳ انتظر {wait_time} ثانية ثم حاول مجدداً"})
        except RPCError as rpc_err:
            err_msg = str(rpc_err)
            if "PHONE_NUMBER_INVALID" in err_msg:
                return jsonify({"success": False, "message": "❌ رقم الهاتف غير صالح"})
            elif "PHONE_NUMBER_FLOOD" in err_msg:
                return jsonify({"success": False, "message": "⚠️ تم إرسال طلبات كثيرة لهذا الرقم، انتظر بضع دقائق"})
            else:
                return jsonify({"success": False, "message": f"❌ خطأ: {err_msg[:100]}"})
        except Exception as send_err:
            logger.error(f"Send code error: {send_err}")
            return jsonify({"success": False, "message": f"❌ فشل إرسال الكود: {str(send_err)[:100]}"})

        with USERS_LOCK:
            ud.awaiting_code = True
            ud.phone_code_hash = sent.phone_code_hash
            ud.phone_number = phone
            ud.connected = True

        settings = load_settings(uid)
        settings['phone'] = phone
        save_settings(uid, settings)

        socketio.emit('log_update', {"message": "📱 تم إرسال كود التحقق - جارٍ الاستلام التلقائي..."}, to=uid)

        try:
            asyncio.run_coroutine_threadsafe(
                ud.client_manager._start_code_listener(),
                ud.client_manager.loop
            )
        except Exception as cl_err:
            logger.warning(f"Code listener start error: {cl_err}")

        return jsonify({"success": True, "message": "📱 تم إرسال كود التحقق", "status": "code_sent"})

    except Exception as e:
        logger.error(f"Login error: {e}")
        socketio.emit('log_update', {"message": f"❌ خطأ في تسجيل الدخول: {str(e)[:150]}"}, to=uid)
        return jsonify({"success": False, "message": f"❌ خطأ: {str(e)[:150]}"})


@app.route("/api/verify_code", methods=["POST"])
def api_verify_code():
    uid = get_current_user_id()
    data = request.json or {}
    code = data.get('code', '').strip()

    if not code:
        return jsonify({"success": False, "message": "أدخل كود التحقق"})

    try:
        from telethon.errors import SessionPasswordNeededError, PhoneCodeInvalidError, PhoneCodeExpiredError

        ud = get_or_create_user(uid)
        if not ud.client_manager or not ud.awaiting_code:
            return jsonify({"success": False, "message": "❌ لا يوجد طلب كود نشط"})

        user = ud.client_manager.run_coroutine(
            ud.client_manager.client.sign_in(ud.phone_number, code, phone_code_hash=ud.phone_code_hash)
        )

        me = ud.client_manager.run_coroutine(ud.client_manager.client.get_me())
        name = (me.first_name or '') + (' ' + (me.last_name or '') if me.last_name else '')
        if not name.strip() and me.username:
            name = f"@{me.username}"
        elif not name.strip():
            name = me.phone or str(me.id)

        with USERS_LOCK:
            ud.authenticated = True
            ud.connected = True
            ud.awaiting_code = False
            ud.telegram_name = name

        ud.client_manager.run_coroutine(ud.client_manager._register_event_handlers())
        socketio.emit('log_update', {"message": "✅ تم تسجيل الدخول بنجاح"}, to=uid)
        return jsonify({"success": True, "message": "✅ تم تسجيل الدخول بنجاح", "status": "success", "telegram_name": name})

    except Exception as e:
        err_name = type(e).__name__
        if 'SessionPasswordNeeded' in err_name:
            with USERS_LOCK:
                ud = USERS.get(uid)
                if ud:
                    ud.awaiting_code = False
                    ud.awaiting_password = True
            return jsonify({"success": True, "message": "🔒 أدخل كلمة مرور التحقق بخطوتين", "status": "password_required"})
        elif 'PhoneCodeInvalid' in err_name:
            return jsonify({"success": False, "message": "❌ كود غير صحيح"})
        elif 'PhoneCodeExpired' in err_name:
            return jsonify({"success": False, "message": "❌ انتهت صلاحية الكود"})
        return jsonify({"success": False, "message": f"❌ {str(e)}", "status": "error"})


@app.route("/api/verify_password", methods=["POST"])
def api_verify_password():
    uid = get_current_user_id()
    data = request.json or {}
    password = data.get('password', '')

    if not password:
        return jsonify({"success": False, "message": "أدخل كلمة المرور"})

    try:
        from telethon.errors import PasswordHashInvalidError

        ud = get_or_create_user(uid)
        if not ud.client_manager:
            return jsonify({"success": False, "message": "❌ العميل غير متصل"})

        ud.client_manager.run_coroutine(
            ud.client_manager.client.sign_in(password=password)
        )

        me = ud.client_manager.run_coroutine(ud.client_manager.client.get_me())
        name = (me.first_name or '') + (' ' + (me.last_name or '') if me.last_name else '')
        if not name.strip() and me.username:
            name = f"@{me.username}"
        elif not name.strip():
            name = me.phone or str(me.id)

        with USERS_LOCK:
            ud.authenticated = True
            ud.connected = True
            ud.awaiting_password = False
            ud.telegram_name = name

        ud.client_manager.run_coroutine(ud.client_manager._register_event_handlers())
        socketio.emit('log_update', {"message": "✅ تم التحقق من كلمة المرور"}, to=uid)
        return jsonify({"success": True, "message": "✅ تم تسجيل الدخول بنجاح", "telegram_name": name})

    except Exception as e:
        err_name = type(e).__name__
        if 'PasswordHashInvalid' in err_name:
            return jsonify({"success": False, "message": "❌ كلمة المرور غير صحيحة"})
        return jsonify({"success": False, "message": f"❌ {str(e)}"})


@app.route("/api/reset_login", methods=["POST"])
def api_reset_login():
    uid = get_current_user_id()
    try:
        ud = get_or_create_user(uid)
        if ud.client_manager:
            ud.client_manager.stop()
            ud.client_manager = None

        session_file = os.path.join(SESSIONS_DIR, f"{uid}_session.session")
        if os.path.exists(session_file):
            os.remove(session_file)

        with USERS_LOCK:
            ud.authenticated = False
            ud.connected = False
            ud.awaiting_code = False
            ud.awaiting_password = False
            ud.phone_code_hash = None
            ud.is_running = False
            ud.monitoring_active = False
            ud.telegram_name = None

        socketio.emit('log_update', {"message": "🔓 تم تسجيل الخروج"}, to=uid)
        return jsonify({"success": True, "message": "✅ تم تسجيل الخروج"})
    except Exception as e:
        return jsonify({"success": False, "message": str(e)})


@app.route("/api/save_settings", methods=["POST"])
def api_save_settings():
    uid = get_current_user_id()
    data = request.json or {}
    settings = load_settings(uid)
    settings.update(data)
    save_settings(uid, settings)
    with USERS_LOCK:
        ud = USERS.get(uid)
        if ud:
            ud.settings = settings
    return jsonify({"success": True, "message": "✅ تم حفظ الإعدادات"})


@app.route("/api/save_auto_replies", methods=["POST"])
def api_save_auto_replies():
    uid = get_current_user_id()
    data = request.json or {}
    auto_replies = data.get('auto_replies', [])
    settings = load_settings(uid)
    settings['auto_replies'] = auto_replies
    save_settings(uid, settings)
    with USERS_LOCK:
        ud = USERS.get(uid)
        if ud:
            ud.auto_replies = auto_replies
            ud.settings = settings
    return jsonify({"success": True, "message": f"✅ تم حفظ {len(auto_replies)} قاعدة رد تلقائي"})


@app.route("/api/upload_image", methods=["POST"])
def api_upload_image():
    uid = get_current_user_id()
    if 'image' not in request.files:
        return jsonify({"success": False, "message": "لا توجد صورة"})

    file = request.files['image']
    if not file.filename:
        return jsonify({"success": False, "message": "اختر ملفاً"})

    ext = os.path.splitext(file.filename)[1].lower()
    if ext not in ['.jpg', '.jpeg', '.png', '.gif', '.webp']:
        return jsonify({"success": False, "message": "صيغة غير مدعومة"})

    filename = f"{uid}_{int(time.time())}{ext}"
    filepath = os.path.join(UPLOADS_DIR, filename)
    file.save(filepath)

    settings = load_settings(uid)
    settings['image_path'] = filepath
    settings['image_filename'] = file.filename
    settings['image_paths'] = [filepath]
    save_settings(uid, settings)

    with USERS_LOCK:
        ud = USERS.get(uid)
        if ud:
            ud.settings = settings

    return jsonify({
        "success": True,
        "message": "✅ تم رفع الصورة",
        "filepath": filepath,
        "filename": file.filename
    })


@app.route("/api/remove_image", methods=["POST"])
def api_remove_image():
    uid = get_current_user_id()
    settings = load_settings(uid)
    img = settings.get('image_path')
    if img and os.path.exists(img):
        try:
            os.remove(img)
        except:
            pass
    settings.pop('image_path', None)
    settings.pop('image_filename', None)
    settings.pop('image_paths', None)
    save_settings(uid, settings)
    return jsonify({"success": True, "message": "✅ تم حذف الصورة"})


@app.route("/api/reset_stats", methods=["POST"])
def api_reset_stats():
    uid = get_current_user_id()
    ud = get_or_create_user(uid)
    with USERS_LOCK:
        ud.stats = {"sent": 0, "errors": 0, "alerts": 0, "replies": 0}
    socketio.emit('stats_update', dict(ud.stats), to=uid)
    return jsonify({"success": True, "message": "✅ تم إعادة تعيين الإحصائيات"})


@app.route("/api/sent_batches", methods=["GET"])
def api_sent_batches():
    uid = get_current_user_id()
    ud = get_or_create_user(uid)
    with USERS_LOCK:
        batches = list(ud.sent_batches)
    result = []
    for b in reversed(batches):
        result.append({
            "id": b["id"], "text": b["text"], "has_media": b.get("has_media", False),
            "sent_at": b["sent_at"], "edited_at": b.get("edited_at"),
            "sent_count": b.get("sent_count", len(b["entries"])),
            "group_count": len(b["entries"]),
            "groups": [{"title": e["chat_title"], "username": e.get("chat_username")} for e in b["entries"]]
        })
    return jsonify({"success": True, "batches": result})


@app.route("/api/edit_batch", methods=["POST"])
def api_edit_batch():
    uid = get_current_user_id()
    ud = get_or_create_user(uid)
    if not ud.authenticated:
        return jsonify({"success": False, "message": "❌ يجب تسجيل الدخول أولاً"})
    data = request.json or {}
    batch_id = data.get("batch_id", "")
    new_text = data.get("new_text", "")
    if not batch_id or not new_text:
        return jsonify({"success": False, "message": "❌ بيانات ناقصة"})
    if not ud.client_manager:
        return jsonify({"success": False, "message": "❌ الاتصال غير جاهز"})

    def run_edit():
        try:
            ud.client_manager.run_coroutine(ud.client_manager._edit_batch_messages(batch_id, new_text), timeout=120)
        except Exception as e:
            socketio.emit('log_update', {"message": f"❌ خطأ في التعديل: {str(e)[:100]}"}, to=uid)

    threading.Thread(target=run_edit, daemon=True).start()
    return jsonify({"success": True, "message": "⏳ جارٍ تعديل الرسائل..."})


@app.route("/api/delete_batch", methods=["POST"])
def api_delete_batch():
    uid = get_current_user_id()
    ud = get_or_create_user(uid)
    if not ud.authenticated:
        return jsonify({"success": False, "message": "❌ يجب تسجيل الدخول أولاً"})
    data = request.json or {}
    batch_id = data.get("batch_id", "")
    if not batch_id:
        return jsonify({"success": False, "message": "❌ batch_id مطلوب"})
    if not ud.client_manager:
        return jsonify({"success": False, "message": "❌ الاتصال غير جاهز"})

    def run_delete():
        try:
            ud.client_manager.run_coroutine(ud.client_manager._delete_batch_messages(batch_id), timeout=120)
        except Exception as e:
            socketio.emit('log_update', {"message": f"❌ خطأ في الحذف: {str(e)[:100]}"}, to=uid)

    threading.Thread(target=run_delete, daemon=True).start()
    return jsonify({"success": True, "message": "⏳ جارٍ حذف الرسائل..."})


@app.route("/api/send_now", methods=["POST"])
def api_send_now():
    uid = get_current_user_id()
    ud = get_or_create_user(uid)

    if not ud.authenticated:
        return jsonify({"success": False, "message": "❌ يجب تسجيل الدخول أولاً"})

    data = request.json or {}
    raw_groups = data.get('raw_groups', '')
    groups = data.get('groups', [])
    message = data.get('message', '')

    if raw_groups and not groups:
        groups = parse_entities(raw_groups)

    if not groups:
        return jsonify({"success": False, "message": "❌ لم يتم العثور على أي مجموعات صالحة"})

    settings = load_settings(uid)
    image_path = settings.get('image_path')

    if not ud.client_manager:
        return jsonify({"success": False, "message": "❌ يجب تسجيل الدخول وتهيئة الاتصال أولاً"})

    def send_async():
        try:
            ud.client_manager.run_coroutine(
                ud.client_manager._send_to_groups(groups, message, image_path),
                timeout=300
            )
        except Exception as e:
            socketio.emit('log_update', {"message": f"❌ خطأ في الإرسال: {str(e)[:150]}"}, to=uid)

    threading.Thread(target=send_async, daemon=True).start()
    return jsonify({"success": True, "message": f"✅ جارٍ الإرسال إلى {len(groups)} مجموعة..."})


@app.route("/api/start_monitoring", methods=["POST"])
def api_start_monitoring():
    uid = get_current_user_id()
    ud = get_or_create_user(uid)

    if not ud.authenticated:
        return jsonify({"success": False, "message": "❌ يجب تسجيل الدخول أولاً"})

    fresh_settings = load_settings(uid)
    with USERS_LOCK:
        ud.monitoring_active = True
        ud.is_running = True
        ud.settings = fresh_settings

    watch_words = fresh_settings.get('watch_words', [])

    if ud.client_manager and ud.client_manager.loop and not ud.client_manager.event_handlers_registered:
        try:
            asyncio.run_coroutine_threadsafe(ud.client_manager._register_event_handlers(), ud.client_manager.loop)
            logger.info(f"Re-registered event handlers for {uid}")
        except Exception as reg_err:
            logger.warning(f"Could not register handlers: {reg_err}")

    socketio.emit('log_update', {"message": f"🚀 بدأت المراقبة - {len(watch_words)} كلمة مراقبة: {', '.join(watch_words[:5])}"}, to=uid)
    socketio.emit('monitoring_status', {"is_running": True, "monitoring_active": True}, to=uid)
    return jsonify({"success": True, "message": f"✅ تم تشغيل المراقبة لـ {len(watch_words)} كلمة"})


@app.route("/api/stop_monitoring", methods=["POST"])
def api_stop_monitoring():
    uid = get_current_user_id()
    ud = get_or_create_user(uid)
    with USERS_LOCK:
        ud.monitoring_active = False
        ud.is_running = False
    socketio.emit('log_update', {"message": "⏹ تم إيقاف المراقبة"}, to=uid)
    socketio.emit('monitoring_status', {"is_running": False, "monitoring_active": False}, to=uid)
    return jsonify({"success": True, "message": "✅ تم إيقاف المراقبة"})


@app.route("/api/start_scheduled", methods=["POST"])
def api_start_scheduled():
    uid = get_current_user_id()
    ud = get_or_create_user(uid)

    if not ud.authenticated:
        return jsonify({"success": False, "message": "❌ يجب تسجيل الدخول أولاً"})

    data = request.json or {}
    groups = data.get('groups', [])
    message = data.get('message', '')
    interval = int(data.get('interval', 60))

    if not groups:
        return jsonify({"success": False, "message": "❌ أضف مجموعات أولاً"})

    settings = load_settings(uid)
    image_path = settings.get('image_path')

    ud.client_manager.start_scheduled(groups, message, image_path, interval)
    return jsonify({"success": True, "message": f"✅ بدأ الإرسال المجدول كل {interval} دقيقة"})


@app.route("/api/stop_scheduled", methods=["POST"])
def api_stop_scheduled():
    uid = get_current_user_id()
    ud = get_or_create_user(uid)
    if ud.client_manager:
        ud.client_manager.stop_scheduled()
    return jsonify({"success": True, "message": "✅ تم إيقاف الإرسال المجدول"})


@app.route("/api/join_group", methods=["POST"])
def api_join_group():
    uid = get_current_user_id()
    ud = get_or_create_user(uid)

    if not ud.authenticated:
        return jsonify({"success": False, "message": "❌ يجب تسجيل الدخول أولاً"})

    data = request.json or {}
    link = data.get('link', '').strip()

    if not link:
        return jsonify({"success": False, "message": "أدخل رابط المجموعة"})

    async def do_join():
        from telethon import functions
        try:
            if 't.me/+' in link or 'joinchat' in link:
                hash_part = link.split('+')[-1] if '+' in link else link.split('/')[-1]
                await ud.client_manager.client(functions.messages.ImportChatInviteRequest(hash=hash_part))
            else:
                entity = link.split('/')[-1]
                await ud.client_manager.client(functions.channels.JoinChannelRequest(channel=entity))
            return {"success": True}
        except Exception as e:
            return {"success": False, "error": str(e)}

    try:
        result = ud.client_manager.run_coroutine(do_join())
        if result['success']:
            socketio.emit('log_update', {"message": f"✅ تم الانضمام إلى {link}"}, to=uid)
            return jsonify({"success": True, "message": "✅ تم الانضمام بنجاح"})
        else:
            return jsonify({"success": False, "message": f"❌ {result.get('error', 'خطأ')}"})
    except Exception as e:
        return jsonify({"success": False, "message": f"❌ {str(e)}"})


@app.route("/api/parse_join_links", methods=["POST"])
def api_parse_join_links():
    data = request.json or {}
    raw = data.get('raw', '')
    entities = parse_entities(raw)

    result = []
    seen = set()
    for e in entities:
        key = e.lower().lstrip('@+')
        if key not in seen:
            seen.add(key)
            if e.startswith('+'):
                kind = 'invite'
                label = f'رابط دعوة: ...{e[-8:]}'
            elif e.lstrip('-').isdigit():
                kind = 'id'
                label = f'ID: {e}'
            else:
                kind = 'username'
                label = f'@{e.lstrip("@")}'
            result.append({"entity": e, "kind": kind, "label": label})

    return jsonify({"success": True, "count": len(result), "links": result})


@app.route("/api/bulk_join", methods=["POST"])
def api_bulk_join():
    uid = get_current_user_id()
    ud = get_or_create_user(uid)

    if not ud.authenticated:
        return jsonify({"success": False, "message": "❌ يجب تسجيل الدخول أولاً"})

    data = request.json or {}
    links = data.get('links', [])

    if not links:
        return jsonify({"success": False, "message": "❌ لا توجد روابط للانضمام"})
    if not ud.client_manager:
        return jsonify({"success": False, "message": "❌ الاتصال غير جاهز"})

    async def do_bulk_join():
        from telethon import functions
        ok = skip = fail = 0
        total = len(links)
        for i, item in enumerate(links):
            entity_str = item.get('entity', '').strip()
            label = item.get('label', entity_str)
            try:
                if entity_str.startswith('+'):
                    try:
                        await ud.client_manager.client(functions.messages.ImportChatInviteRequest(hash=entity_str[1:]))
                        ok += 1
                        msg = f"✅ [{i+1}/{total}] {label}"
                    except Exception as je:
                        if 'Already' in str(je) or 'USER_ALREADY' in str(je):
                            skip += 1
                            msg = f"⚠️ [{i+1}/{total}] مسجّل مسبقاً: {label}"
                        else:
                            raise je
                elif entity_str.lstrip('-').isdigit():
                    chat = await ud.client_manager.client.get_entity(int(entity_str))
                    await ud.client_manager.client(functions.channels.JoinChannelRequest(channel=chat))
                    ok += 1
                    msg = f"✅ [{i+1}/{total}] {label}"
                else:
                    username = entity_str.lstrip('@')
                    await ud.client_manager.client(functions.channels.JoinChannelRequest(channel=username))
                    ok += 1
                    msg = f"✅ [{i+1}/{total}] @{username}"
                socketio.emit('log_update', {"message": msg}, to=uid)
                socketio.emit('join_progress', {"index": i+1, "total": total, "ok": ok, "skip": skip, "fail": fail}, to=uid)
                await asyncio.sleep(2)
            except Exception as e:
                fail += 1
                err = str(e)
                if 'Already' in err or 'USER_ALREADY' in err:
                    skip += 1
                    fail -= 1
                    socketio.emit('log_update', {"message": f"⚠️ [{i+1}/{total}] مسجّل: {label}"}, to=uid)
                else:
                    socketio.emit('log_update', {"message": f"❌ [{i+1}/{total}] {label}: {err[:60]}"}, to=uid)
                socketio.emit('join_progress', {"index": i+1, "total": total, "ok": ok, "skip": skip, "fail": fail}, to=uid)
                await asyncio.sleep(1)

        socketio.emit('bulk_join_done', {"ok": ok, "skip": skip, "fail": fail, "total": total}, to=uid)
        socketio.emit('log_update', {"message": f"🏁 اكتمل: ✅ {ok} | ⚠️ {skip} مسبقاً | ❌ {fail} فاشل من {total}"}, to=uid)

    def run_bulk():
        try:
            ud.client_manager.run_coroutine(do_bulk_join(), timeout=600)
        except Exception as e:
            socketio.emit('log_update', {"message": f"❌ خطأ: {str(e)[:100]}"}, to=uid)
            socketio.emit('bulk_join_done', {"ok": 0, "skip": 0, "fail": len(links), "total": len(links)}, to=uid)

    threading.Thread(target=run_bulk, daemon=True).start()
    return jsonify({"success": True, "message": f"⏳ جارٍ الانضمام إلى {len(links)} مجموعة..."})


@socketio.on('connect')
def handle_connect():
    uid = session.get('user_id', 'user_1')
    if uid not in PREDEFINED_USERS:
        uid = 'user_1'
        session['user_id'] = uid
    join_room(uid)
    get_or_create_user(uid)
    emit('connection_confirmed', {'user_id': uid})
    logger.info(f"✅ Connected: {uid}")


@socketio.on('join_user_room')
def handle_join_room(data):
    uid = data.get('user_id', session.get('user_id', 'user_1'))
    if uid in PREDEFINED_USERS:
        join_room(uid)


@socketio.on('disconnect')
def handle_disconnect():
    uid = session.get('user_id', 'user_1')
    leave_room(uid)


@socketio.on('heartbeat')
def handle_heartbeat(data):
    pass


def load_all_sessions():
    logger.info("Loading existing sessions...")
    for filename in os.listdir(SESSIONS_DIR):
        if filename.endswith('.json'):
            uid = filename.split('.')[0]
            if uid in PREDEFINED_USERS:
                settings = load_settings(uid)
                if settings.get('phone'):
                    ud = get_or_create_user(uid)
                    logger.info(f"Loaded settings for {uid}")


if __name__ == '__main__':
    load_all_sessions()
    port = int(os.environ.get('PORT', 5000))
    logger.info(f"🚀 Starting on port {port}")
    socketio.run(app, host='0.0.0.0', port=port, debug=False, allow_unsafe_werkzeug=True)
