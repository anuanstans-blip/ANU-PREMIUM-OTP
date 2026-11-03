import asyncio
import logging
import os
import re
import random
import json
import pyotp
import httpx
from keep_alive import keep_alive
from supabase import create_client, Client
from telegram import (
    Update,
    ReplyKeyboardMarkup,
    KeyboardButton,
    InlineKeyboardButton,
    InlineKeyboardMarkup,
    CopyTextButton,
)
from telegram.ext import (
    ApplicationBuilder,
    CommandHandler,
    MessageHandler,
    CallbackQueryHandler,
    ContextTypes,
    filters,
)

# --- Configuration via Environment Variables ---
TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "").strip(" '\"")

_admin_raw = os.getenv("ADMIN_ID", "0").strip(" '\"")
ADMIN_ID = int(_admin_raw) if _admin_raw.isdigit() else 0

_channel_raw = os.getenv("CHANNEL_CHAT_ID", "0").strip(" '\"")
CHANNEL_CHAT_ID = int(_channel_raw) if _channel_raw.lstrip("-").isdigit() else 0

CHANNEL_URL = os.getenv("CHANNEL_URL", "https://t.me/").strip(" '\"")

ZEBRA_API_KEY = os.getenv("ZEBRA_API_KEY", "").strip(" '\"")
ZEBRA_BASE_URL = os.getenv("ZEBRA_BASE_URL", "https://api.zebrasms.com/api/v1").strip(" '\"")

SUPPORT_USERNAME = os.getenv("SUPPORT_USERNAME", "@anstans").strip(" '\"")

SUPABASE_URL = os.getenv("SUPABASE_URL", "").strip(" '\"")
SUPABASE_KEY = os.getenv("SUPABASE_KEY", "").strip(" '\"")

# Initialize Supabase Client
supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY) if SUPABASE_URL and SUPABASE_KEY else None

logging.basicConfig(
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    level=logging.INFO,
)

# --- Global State & Async Queue ---
active_allocations = {}
seen_messages = set()
MAX_SEEN_SIZE = 5000

otp_dispatch_queue = asyncio.Queue()
active_worker_tasks = {}

fake_otp_settings = {
    "enabled": False,
    "interval": 15,
}

DEFAULT_RANGES_ZEBRA = [
    {"service": "WhatsApp", "country": "Ivory Coast", "flag": "🇨🇮", "range": "22501XXX"},
]


# --- Name Generator Helper ---
def generate_random_english_name() -> tuple[str, str]:
    first_names = [
        "Getu", "Ashenafi", "Noah", "Darge", "Ethan", "Bereket", "Mason", "Logan",
        "Emma", "Sophia", "Olivia", "Mewuded", "Mia", "Isabella", "Belayneh", "Kalkidan",
        "Mekdes", "Benjamin", "Elijah", "Girma", "Charlotte", "Amelia", "Tagay"
    ]
    last_names = [
        "Geremew", "Abriham", "Williams", "Kassa", "Jones", "Kondara", "Dawit",
        "Wilson", "Taylor", "Anderson", "Mulugeta", "Dachasa", "White", "Harris",
        "Martin", "Takele", "Garcia", "Martinez", "Munuro", "Asaminew", "Rodriguez"
    ]
    return random.choice(first_names), random.choice(last_names)


# --- Database Helper Functions ---

def db_add_user(user_id: int):
    if not supabase:
        return
    try:
        supabase.table("users").upsert({"user_id": user_id}, on_conflict="user_id").execute()
    except Exception as e:
        logging.error(f"Error saving user to Supabase: {e}")


def db_get_all_users() -> list:
    if not supabase:
        return []
    try:
        response = supabase.table("users").select("user_id").execute()
        return [row["user_id"] for row in response.data] if response.data else []
    except Exception as e:
        logging.error(f"Error fetching users for broadcast: {e}")
        return []


def db_get_managed_ranges(provider: str) -> list:
    if not supabase:
        return DEFAULT_RANGES_ZEBRA
    try:
        response = supabase.table("managed_ranges").select("*").eq("provider", provider).execute()
        if response.data:
            return response.data
        return []
    except Exception as e:
        logging.error(f"Error fetching ranges from Supabase: {e}")
        return []


def db_get_all_managed_ranges() -> list:
    if not supabase:
        return [{**r, "provider": "zebrasms"} for r in DEFAULT_RANGES_ZEBRA]
    try:
        response = supabase.table("managed_ranges").select("*").execute()
        return response.data if response.data else []
    except Exception as e:
        logging.error(f"Error fetching all ranges from Supabase: {e}")
        return []


def db_add_managed_range(service: str, country: str, flag: str, range_val: str, provider: str):
    if not supabase:
        return
    try:
        data = {
            "service": service,
            "country": country,
            "flag": flag,
            "range": range_val,
            "provider": provider,
        }
        supabase.table("managed_ranges").upsert(data, on_conflict="service, range").execute()
    except Exception as e:
        logging.error(f"Error inserting range to Supabase: {e}")


def db_delete_specific_range(range_val: str, service: str = None):
    if not supabase:
        return
    try:
        query = supabase.table("managed_ranges").delete().eq("range", range_val)
        if service:
            query = query.eq("service", service)
        query.execute()
    except Exception as e:
        logging.error(f"Error deleting range from Supabase: {e}")


def db_clear_managed_ranges(provider: str):
    if not supabase:
        return
    try:
        supabase.table("managed_ranges").delete().eq("provider", provider).execute()
    except Exception as e:
        logging.error(f"Error clearing ranges from Supabase: {e}")


# --- Country Mapping ---
COUNTRY_FLAG_MAP = {
    "ivory coast": ("Ivory Coast", "🇨🇮"), "ci": ("Ivory Coast", "🇨🇮"), "225": ("Ivory Coast", "🇨🇮"),
    "ethiopia": ("Ethiopia", "🇪🇹"), "et": ("Ethiopia", "🇪🇹"), "251": ("Ethiopia", "🇪🇹"),
    "italy": ("Italy", "🇮🇹"), "it": ("Italy", "🇮🇹"), "39": ("Italy", "🇮🇹"),
}


def auto_detect_country_and_flag(country_text: str, phone_or_range: str) -> tuple[str, str]:
    text_clean = (country_text or "").strip().lower()
    digits_only = re.sub(r"\D", "", phone_or_range or "")

    if text_clean in COUNTRY_FLAG_MAP:
        return COUNTRY_FLAG_MAP[text_clean]

    for prefix in sorted([k for k in COUNTRY_FLAG_MAP.keys() if k.isdigit()], key=len, reverse=True):
        if digits_only.startswith(prefix):
            return COUNTRY_FLAG_MAP[prefix]

    return country_text if country_text else "Unknown", "🌐"


def mask_phone_number(phone: str) -> str:
    digits = re.sub(r"\D", "", phone)
    if len(digits) >= 9:
        return f"{digits[:3]}{digits[3:6]}ⒶⓃⓊ{digits[-2:]}"
    elif len(digits) >= 6:
        return f"{digits[:3]}ANU{digits[-2:]}"
    return phone


def extract_code(message_text: str) -> str:
    match = re.search(r"\b\d{4,8}\b", message_text)
    return match.group(0) if match else "No code found"


# --- ZebraSMS API Client ---

class ZebraSMSClient:
    def __init__(self, api_key: str):
        self.base_url = ZEBRA_BASE_URL.rstrip("/")
        self.headers = {
            "MAuth": api_key,
            "Content-Type": "application/json",
        }
        self.client = httpx.AsyncClient(
            follow_redirects=True, 
            timeout=5.0, 
            limits=httpx.Limits(max_keepalive_connections=50, max_connections=200)
        )

    async def get_live_access(self, sender: str = None) -> dict:
        """GET /publicapi/liveaccess[cite: 16]"""
        try:
            url = f"{self.base_url}/publicapi/liveaccess"
            if sender:
                url += f"?sender={sender}"
            response = await self.client.get(url, headers=self.headers)
            if response.status_code != 200:
                return {"meta": {"code": -500}, "message": f"HTTP {response.status_code}"}
            return response.json()
        except Exception as e:
            return {"meta": {"code": -500}, "message": str(e)}

    async def get_number(self, range_val: str) -> dict:
        """POST /publicapi/getnum[cite: 16]"""
        try:
            payload = {"range": range_val}
            response = await self.client.post(
                f"{self.base_url}/publicapi/getnum",
                headers=self.headers,
                json=payload,
            )
            if response.status_code != 200:
                return {"meta": {"code": -500}, "message": f"HTTP {response.status_code}"}
            return response.json()
        except Exception as e:
            return {"meta": {"code": -500}, "message": str(e)}

    async def get_updates(self) -> dict:
        """GET /publicapi/getupdate[cite: 16]"""
        try:
            response = await self.client.get(
                f"{self.base_url}/publicapi/getupdate",
                headers=self.headers,
            )
            if response.status_code != 200:
                return {"meta": {"code": -500}, "message": f"HTTP {response.status_code}"}
            return response.json()
        except Exception as e:
            return {"meta": {"code": -500}, "message": str(e)}


zebra_client = ZebraSMSClient(ZEBRA_API_KEY) if ZEBRA_API_KEY else None


async def auto_delete_allocation_message(context: ContextTypes.DEFAULT_TYPE, chat_id: int, message_id: int, alloc_key: str):
    await asyncio.sleep(900)
    if alloc_key in active_allocations:
        active_allocations.pop(alloc_key, None)
        try:
            await context.bot.delete_message(chat_id=chat_id, message_id=message_id)
        except Exception as e:
            logging.error(f"Error auto-deleting message: {e}")


async def cmd_test_sms(update: Update, context: ContextTypes.DEFAULT_TYPE):
    dummy_number = "+225010000000"
    dummy_sender = "VERIFY"
    dummy_message = "Your code is 1234"
    country_name, flag_icon = auto_detect_country_and_flag("Ivory Coast", dummy_number)

    code = extract_code(dummy_message)
    masked_num = mask_phone_number(dummy_number)

    dm_text = (
        "📩 *[TEST] ⒶⓃⓊ ⓄⓉⓅ Ⓡⓔⓒⓔⓘⓥⓔⓓ!*\n\n"
        f"📱 *To Number:* `{dummy_number}`\n"
        f"👤 *Sender:* `{dummy_sender}`\n"
        f"🔑 *Code:* `{code}`"
    )
    copy_keyboard = InlineKeyboardMarkup([
        [InlineKeyboardButton(f"📋 Copy OTP: {code}", copy_text=CopyTextButton(text=str(code)))]
    ])
    await update.message.reply_text(dm_text, parse_mode="Markdown", reply_markup=copy_keyboard)


# --- Dispatch Consumer Queue ---
async def dispatch_consumer_loop(app):
    while True:
        try:
            item = await otp_dispatch_queue.get()
            allocation_info, target_number, otp_code, sender, msg_text = item
            
            msg_id = f"{target_number}_{otp_code}_{msg_text}"
            if len(seen_messages) > MAX_SEEN_SIZE:
                seen_messages.clear()
            if msg_id in seen_messages:
                otp_dispatch_queue.task_done()
                continue
            seen_messages.add(msg_id)

            user_chat_id = allocation_info["chat_id"]
            country_name = allocation_info["country"]
            flag_icon = allocation_info["flag"]
            user_fname = allocation_info.get("first_name", "User")

            dm_text = (
                f"📩 *Hey {user_fname}, your OTP has arrived!*\n\n"
                f"📱 *To Number:* `{target_number}`\n"
            )
            dm_keyboard = InlineKeyboardMarkup([
                [InlineKeyboardButton(f"📋 Copy OTP: {otp_code}", copy_text=CopyTextButton(text=str(otp_code)))]
            ])

            try:
                await app.bot.send_message(chat_id=user_chat_id, text=dm_text, parse_mode="Markdown", reply_markup=dm_keyboard)
            except Exception as e:
                logging.error(f"Error sending DM: {e}")

            if CHANNEL_CHAT_ID:
                masked_num = mask_phone_number(target_number)
                channel_text = (
                    "📢 *ANU OTP RECEIVED*\n\n"
                    f"📱 *To Number:* `{masked_num}`\n"
                    f"{flag_icon} *Country:* {country_name}\n"
                    f"👤 *Sender:* `{sender}`\n"
                    f"💬 *Full Message:* `{msg_text}`"
                )
                channel_keyboard = InlineKeyboardMarkup([
                    [InlineKeyboardButton(f"📋 Copy OTP: {otp_code}", copy_text=CopyTextButton(text=str(otp_code)))],
                    [InlineKeyboardButton("👨‍💻 Developer", url="https://t.me/anstans")]
                ])
                try:
                    await app.bot.send_message(chat_id=CHANNEL_CHAT_ID, text=channel_text, parse_mode="Markdown", reply_markup=channel_keyboard)
                except Exception as err:
                    logging.error(f"Failed to post to channel: {err}")

            otp_dispatch_queue.task_done()
        except Exception as e:
            logging.error(f"Error in consumer loop: {e}")
            await asyncio.sleep(0.1)


async def dispatch_otp(app, allocation_info, target_number, otp_code, sender, msg_text):
    await otp_dispatch_queue.put((allocation_info, target_number, otp_code, sender, msg_text))


# --- Polling Worker using ZebraSMS getupdate ---
async def poll_single_allocation(app, alloc_key):
    """Polls global getupdate feed to check for incoming SMS on the allocated number."""
    start_time = asyncio.get_event_loop().time()

    while alloc_key in active_allocations:
        if asyncio.get_event_loop().time() - start_time > 900:
            active_allocations.pop(alloc_key, None)
            break

        info = active_allocations.get(alloc_key)
        if not info:
            break

        target_phone = info["number"]

        try:
            if zebra_client:
                res = await zebra_client.get_updates()
                if res.get("meta", {}).get("code") == 0:
                    rows = res.get("data", {}).get("rows", [])
                    for row in rows:
                        # Match message destination number
                        if row.get("number") == target_phone:
                            msg_text = row.get("message", "")
                            otp_code = extract_code(msg_text)
                            if otp_code != "No code found":
                                await dispatch_otp(app, info, target_phone, otp_code, row.get("sender", "SMS"), msg_text)
                                active_allocations.pop(alloc_key, None)
                                return
        except Exception as e:
            logging.error(f"Error polling updates for {alloc_key}: {e}")

        await asyncio.sleep(3.0)

    active_worker_tasks.pop(alloc_key, None)


async def auto_check_updates(app):
    while True:
        try:
            current_keys = list(active_allocations.keys())
            for key in current_keys:
                if key not in active_worker_tasks or active_worker_tasks[key].done():
                    active_worker_tasks[key] = asyncio.create_task(poll_single_allocation(app, key))
        except Exception as e:
            logging.error(f"Error in manager task loop: {e}")

        await asyncio.sleep(0.3)


def get_main_keyboard():
    keyboard = [
        [KeyboardButton("📱 Get Number 🟢"), KeyboardButton("⚡ Active Engine ⚡")],
        [KeyboardButton("🌐 Live Feed 🔵"), KeyboardButton("🎁 Referrals 🟡")],
        [KeyboardButton("🔐 2FA Online 🟣"), KeyboardButton("🎧 Support Hub 🔴")],
    ]
    return ReplyKeyboardMarkup(keyboard, resize_keyboard=True)


async def cmd_admin(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    if ADMIN_ID != 0 and user_id != ADMIN_ID:
        await update.message.reply_text(f"⛔ **Access Denied:** Your Telegram ID `{user_id}` is not configured as admin.", parse_mode="Markdown")
        return

    keyboard = [
        [InlineKeyboardButton("🌐 ZebraSMS", callback_data="admin_prov_zebrasms")]
    ]
    await update.message.reply_text("🛠 **Admin Configuration Panel**\n\n👇 *Select provider to manage ranges:*", parse_mode="Markdown", reply_markup=InlineKeyboardMarkup(keyboard))


async def admin_callback_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    user_id = query.from_user.id
    if ADMIN_ID != 0 and user_id != ADMIN_ID:
        await query.answer("Unauthorized action.", show_alert=True)
        return

    data = query.data

    if data.startswith("admin_prov_"):
        prov = data.replace("admin_prov_", "")
        context.user_data["admin_provider"] = prov
        managed_ranges = db_get_managed_ranges(prov)

        ranges_text = "\n".join(
            [f"• {r['flag']} 🔹 **[{r['service']}]** {r['country']} (`{r['range']}`)" for r in managed_ranges]
        ) or "No active ranges configured."

        msg = (
            f"🛠 **Admin Panel: ZebraSMS** 🛠\n\n"
            f"📋 **Current Active Ranges:**\n{ranges_text}\n\n"
            f"👇 *Manage ranges below:*"
        )
        keyboard = [
            [
                InlineKeyboardButton("➕ Add Range 🟢", callback_data=f"admin_add_{prov}"),
                InlineKeyboardButton("🗑 Delete Range 🔴", callback_data=f"admin_del_select_{prov}"),
            ],
            [
                InlineKeyboardButton("⚠️ Clear All 🟠", callback_data=f"admin_clear_{prov}")
            ]
        ]
        await query.edit_message_text(msg, parse_mode="Markdown", reply_markup=InlineKeyboardMarkup(keyboard))
        return

    elif data.startswith("admin_add_"):
        prov = data.replace("admin_add_", "")
        context.user_data["waiting_for_range"] = prov
        await query.message.reply_text(
            f"✍ **Send configuration for ZebraSMS:**\n\n`Service | Country | Range`\n👉 *Example:* `WhatsApp | Ivory Coast | 22501XXX`",
            parse_mode="Markdown",
        )
        await query.answer()

    elif data.startswith("admin_del_select_"):
        prov = data.replace("admin_del_select_", "")
        await query.answer()
        managed_ranges = db_get_managed_ranges(prov)
        if not managed_ranges:
            await query.edit_message_text("❌ No active ranges found to delete.")
            return

        keyboard = [
            [InlineKeyboardButton(f"❌ Delete {r['flag']} {r['service']} ({r['range']}) 🔴", callback_data=f"admin_del_{r['service']}_{r['range']}")]
            for r in managed_ranges
        ]
        await query.edit_message_text(f"🗑 **Select range to delete:**", reply_markup=InlineKeyboardMarkup(keyboard))

    elif data.startswith("admin_del_"):
        parts = data.replace("admin_del_", "").split("_", 1)
        if len(parts) == 2:
            srv_to_del, range_to_del = parts[0], parts[1]
            db_delete_specific_range(range_to_del, service=srv_to_del)
            await query.answer(f"Deleted {srv_to_del} range {range_to_del}", show_alert=True)
            await query.edit_message_text(f"✅ **Range `{range_to_del}` for service `{srv_to_del}` deleted.**", parse_mode="Markdown")

    elif data.startswith("admin_clear_"):
        prov = data.replace("admin_clear_", "")
        db_clear_managed_ranges(prov)
        await query.answer("All ZebraSMS ranges cleared!", show_alert=True)
        await query.edit_message_text("🗑 **All configured ZebraSMS ranges cleared.**")


async def user_provision_callback_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    data = query.data
    chat_id = query.message.chat_id

    if data.startswith("get_srv_"):
        await query.answer()
        selected_service = data.replace("get_srv_", "").strip()
        all_ranges = db_get_all_managed_ranges()
        
        matching_ranges = [r for r in all_ranges if r["service"].upper() == selected_service.upper()]

        if not matching_ranges:
            await query.edit_message_text(f"⚠ No ranges found for service `{selected_service}`.", parse_mode="Markdown")
            return

        country_buttons = [
            InlineKeyboardButton(
                f"{r['flag']} {r['country']} (`{r['range']}`)", 
                callback_data=f"prov_{r['provider']}_{r['range']}"
            )
            for r in matching_ranges
        ]
        keyboard = [country_buttons[i:i + 2] for i in range(0, len(country_buttons), 2)]
        keyboard.append([InlineKeyboardButton("🔙 Back to Services", callback_data="back_to_services")])

        await query.edit_message_text(
            f"🛠 *Selected Service:* `{selected_service}`\n\n👇 *Select Range to allocate number:*",
            parse_mode="Markdown",
            reply_markup=InlineKeyboardMarkup(keyboard),
        )
        return

    elif data == "back_to_services":
        await query.answer()
        all_ranges = db_get_all_managed_ranges()
        if not all_ranges:
            await query.edit_message_text("⚠ No ranges configured yet.", parse_mode="Markdown")
            return

        services = sorted(list(set(r["service"] for r in all_ranges)))
        keyboard = [[InlineKeyboardButton(f"🛡️ {srv} 🟢", callback_data=f"get_srv_{srv}")] for srv in services]
        await query.edit_message_text("📱 *Select a Service:*", parse_mode="Markdown", reply_markup=InlineKeyboardMarkup(keyboard))
        return

    elif data.startswith("prov_") or data.startswith("change_"):
        await query.answer()
        is_change = data.startswith("change_")
        clean_data = data.replace("change_", "") if is_change else data.replace("prov_", "")

        parts = clean_data.split("_", 1)
        chosen_provider = parts[0]
        selected_range = parts[1]

        if is_change:
            try:
                await query.message.delete()
            except Exception as e:
                logging.error(f"Error deleting old number message: {e}")

        all_ranges = db_get_all_managed_ranges()
        matched_item = next((r for r in all_ranges if r["range"] == selected_range), {})
        c_name, flag_icon = auto_detect_country_and_flag(matched_item.get("country", ""), selected_range)

        loading_msg = await context.bot.send_message(
            chat_id=chat_id, 
            text=f"⏳ Requesting fresh number for {flag_icon} range `{selected_range}`...", 
            parse_mode="Markdown"
        )

        allocated_results = []

        if zebra_client:
            await asyncio.sleep(0.3)
            res = await zebra_client.get_number(range_val=selected_range)
            if isinstance(res, dict) and res.get("meta", {}).get("code") == 0:
                rows = res.get("data", {}).get("rows", [])
                if rows:
                    d = rows[0]
                    # Use phone number as unique rental/allocation key for ZebraSMS
                    allocated_results.append((d.get("number"), d.get("number"), d.get("country")))

        if allocated_results:
            first_entry = allocated_results[0]
            target_country = first_entry[2] if first_entry[2] else matched_item.get("country", c_name)
            final_country_name, final_flag = auto_detect_country_and_flag(target_country, first_entry[1])

            rand_fname, rand_lname = generate_random_english_name()
            number_copy_buttons = []

            for alloc_key, allocated_num, _ in allocated_results:
                if alloc_key and allocated_num:
                    active_allocations[alloc_key] = {
                        "chat_id": chat_id,
                        "country": final_country_name,
                        "flag": final_flag,
                        "number": allocated_num,
                        "provider": chosen_provider,
                        "first_name": rand_fname
                    }
                    number_copy_buttons.append(
                        [InlineKeyboardButton(f"📱 {allocated_num}", copy_text=CopyTextButton(text=str(allocated_num)))]
                    )

            msg = (
                f"✅ **Allocated!**\n\n"
                f"👤 **First Name:** `{rand_fname}`\n"
                f"👤 **Last Name:** `{rand_lname}`\n"
                f"{final_flag} **Country:** {final_country_name}\n"
                f"⏱ **Expires In:** ~15 mins"
            )

            keyboard_layout = number_copy_buttons + [
                [InlineKeyboardButton("🔄 Change Number 🟠", callback_data=f"change_{chosen_provider}_{selected_range}")],
                [InlineKeyboardButton("📢 Open OTP Channel 🔵", url=CHANNEL_URL)],
            ]
            keyboard = InlineKeyboardMarkup(keyboard_layout)

            allocated_msg = await loading_msg.edit_text(msg, parse_mode="Markdown", reply_markup=keyboard)

            asyncio.create_task(
                auto_delete_allocation_message(
                    context=context,
                    chat_id=chat_id,
                    message_id=allocated_msg.message_id,
                    alloc_key=first_entry[0]
                )
            )
        else:
            fallback_keyboard = InlineKeyboardMarkup([
                [InlineKeyboardButton("🔄 Try Again", callback_data=data)],
                [InlineKeyboardButton("🔙 Back to Services", callback_data="back_to_services")]
            ])
            await loading_msg.edit_text(
                f"❌ **Out of stock or API rate limit hit.** Tap below to try again or choose another service.",
                parse_mode="Markdown",
                reply_markup=fallback_keyboard
            )


async def start_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    user_name = update.effective_user.first_name or "User"
    db_add_user(user_id)
    
    welcome_msg = (
        f"👋 *ANU PREMIUM OTP BOT*\n\n"
        f"Welcome, *{user_name}*! 👋\n\n"
        f"Need help or want to add a working number? Contact support: {SUPPORT_USERNAME}"
    )
    await update.message.reply_text(welcome_msg, parse_mode="Markdown", reply_markup=get_main_keyboard())


async def handle_text_messages(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_chat_id = update.effective_chat.id
    db_add_user(user_chat_id)

    text = update.message.text.strip() if update.message.text else ""

    if context.user_data.get("waiting_for_range"):
        prov = context.user_data.get("waiting_for_range")
        
        if text.count("|") != 2:
            await update.message.reply_text("⚠️ **Format Error!** Use: `Service | Country | Range`", parse_mode="Markdown")
            return

        parts = [p.strip() for p in text.split("|")]
        service = parts[0].upper()
        country = parts[1]
        range_val = parts[2]

        c_name, detected_flag = auto_detect_country_and_flag(country, range_val)

        db_add_managed_range(service, c_name, detected_flag, range_val, provider=prov)
        context.user_data["waiting_for_range"] = None

        await update.message.reply_text(
            f"✅ **Successfully saved range!**\n📌 Service: `{service}`\n{detected_flag} Country: `{c_name}`\n🔢 Range: `{range_val}`",
            parse_mode="Markdown",
            reply_markup=get_main_keyboard(),
        )
        return

    if "Get Number" in text:
        all_ranges = db_get_all_managed_ranges()
        if not all_ranges:
            await update.message.reply_text("⚠ No ranges configured yet. Add ranges via `/admin`.", parse_mode="Markdown")
            return

        services = sorted(list(set(r["service"] for r in all_ranges)))
        keyboard = [[InlineKeyboardButton(f"🛡️ {srv} 🟢", callback_data=f"get_srv_{srv}")] for srv in services]
        await update.message.reply_text("📱 *Select a Service:*", parse_mode="Markdown", reply_markup=InlineKeyboardMarkup(keyboard))

    elif "Active Engine" in text:
        await update.message.reply_text("⚡ Active delivery engines are online and ready.", parse_mode="Markdown")

    elif "Live Feed" in text:
        await update.message.reply_text("📭 Live feed monitor active.", parse_mode="Markdown")

    elif "Referrals" in text:
        bot_username = (await context.bot.get_me()).username
        await update.message.reply_text(f"🎁 **Referral Link:**\n🔗 `https://t.me/{bot_username}?start={user_chat_id}`", parse_mode="Markdown")

    elif "My Profile" in text:
        user_nums = [info.get("number") for n, info in active_allocations.items() if info["chat_id"] == user_chat_id]
        nums_text = "\n".join([f"• `{num}`" for num in user_nums]) if user_nums else "None"
        await update.message.reply_text(f"👤 **User Profile**\n\n🆔 ID: `{user_chat_id}`\n📱 Active Numbers:\n{nums_text}", parse_mode="Markdown")

    elif "Support Hub" in text:
        await update.message.reply_text(f"🎧 **Support Hub**\n\nContact support agent directly {SUPPORT_USERNAME}", parse_mode="Markdown")


async def post_init(application):
    application.create_task(dispatch_consumer_loop(application))
    application.create_task(auto_check_updates(application))


# --- Main Execution ---

if __name__ == "__main__":
    keep_alive()
    print("Keep-alive HTTP server started.")

    app = ApplicationBuilder().token(TELEGRAM_BOT_TOKEN).post_init(post_init).build()

    app.add_handler(CommandHandler("start", start_command))
    app.add_handler(CommandHandler("admin", cmd_admin))
    app.add_handler(CommandHandler("test_sms", cmd_test_sms))

    app.add_handler(CallbackQueryHandler(admin_callback_handler, pattern="^admin_"))
    app.add_handler(CallbackQueryHandler(user_provision_callback_handler, pattern="^(get_srv_|back_to_services|prov_|change_)"))

    app.add_handler(MessageHandler(filters.ALL & ~filters.COMMAND, handle_text_messages))

    print("🤖 ZebraSMS Bot running successfully...")
    app.run_polling()
