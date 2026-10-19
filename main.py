import asyncio
import logging
import os
import re
import random
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
TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
ADMIN_ID = int(os.getenv("ADMIN_ID", "0"))
CHANNEL_CHAT_ID = int(os.getenv("CHANNEL_CHAT_ID", "0"))
CHANNEL_URL = os.getenv("CHANNEL_URL", "https://t.me/")

MKNETWORK_API_KEY = os.getenv("MKNETWORK_API_KEY")
MKNETWORK_BASE_URL = os.getenv("MKNETWORK_BASE_URL", "https://mknetworkbd.com/api/v2/public")

ZEBRA_API_KEY = os.getenv("ZEBRA_API_KEY")
ZEBRA_BASE_URL = os.getenv("ZEBRA_BASE_URL", "https://api.zebrasms.com/api/v1")

SUPPORT_USERNAME = os.getenv("SUPPORT_USERNAME", "@anstans")

SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")

# Initialize Supabase Client
supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY) if SUPABASE_URL and SUPABASE_KEY else None

logging.basicConfig(
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    level=logging.INFO,
)

# --- Global State ---
active_allocations = {}
seen_messages = set()
MAX_SEEN_SIZE = 5000

# Fake OTP Generator State & Settings
fake_otp_settings = {
    "enabled": False,
    "interval": 15,
}

# Fallback default ranges
DEFAULT_RANGES_MK = [
    {"service": "Facebook", "country": "Sierra Leone", "flag": "🇸🇱", "range": "23276345XXX"},
    {"service": "Telegram", "country": "Sierra Leone", "flag": "🇸🇱", "range": "23276349XXX"},
]
DEFAULT_RANGES_ZEBRA = [
    {"service": "Facebook", "country": "Cambodia", "flag": "🇰🇭", "range": "85531879XXX"},
    {"service": "Telegram", "country": "Ivory Coast", "flag": "🇨🇮", "range": "22501XXX"},
]


# --- Database Helper Functions ---

def db_add_user(user_id: int):
    """Save active users for admin broadcasting."""
    if not supabase:
        return
    try:
        supabase.table("users").upsert({"user_id": user_id}, on_conflict="user_id").execute()
    except Exception as e:
        logging.error(f"Error saving user to Supabase: {e}")


def db_get_all_users() -> list:
    """Retrieve all user IDs for broadcasting."""
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
        return DEFAULT_RANGES_MK if provider == "mknetwork" else DEFAULT_RANGES_ZEBRA
    try:
        response = supabase.table("managed_ranges").select("*").eq("provider", provider).execute()
        if response.data:
            return response.data
        return []
    except Exception as e:
        logging.error(f"Error fetching ranges from Supabase: {e}")
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
        supabase.table("managed_ranges").upsert(data, on_conflict="range").execute()
    except Exception as e:
        logging.error(f"Error inserting range to Supabase: {e}")


def db_delete_specific_range(range_val: str):
    if not supabase:
        return
    try:
        supabase.table("managed_ranges").delete().eq("range", range_val).execute()
    except Exception as e:
        logging.error(f"Error deleting range from Supabase: {e}")


def db_clear_managed_ranges(provider: str):
    if not supabase:
        return
    try:
        supabase.table("managed_ranges").delete().eq("provider", provider).execute()
    except Exception as e:
        logging.error(f"Error clearing ranges from Supabase: {e}")


# --- Comprehensive World Country & Flag Map ---
COUNTRY_FLAG_MAP = {
    # Africa
    "algeria": ("Algeria", "🇩🇿"), "213": ("Algeria", "🇩🇿"),
    "angola": ("Angola", "🇦🇴"), "244": ("Angola", "🇦🇴"),
    "benin": ("Benin", "🇧🇯"), "229": ("Benin", "🇧🇯"),
    "botswana": ("Botswana", "🇧🇼"), "267": ("Botswana", "🇧🇼"),
    "burkina faso": ("Burkina Faso", "🇧🇫"), "226": ("Burkina Faso", "🇧🇫"),
    "burundi": ("Burundi", "🇧🇮"), "257": ("Burundi", "🇧🇮"),
    "cameroon": ("Cameroon", "🇨🇲"), "237": ("Cameroon", "🇨🇲"),
    "cape verde": ("Cape Verde", "🇨🇻"), "238": ("Cape Verde", "🇨🇻"),
    "central african republic": ("Central African Republic", "🇨🇫"), "236": ("Central African Republic", "🇨🇫"),
    "chad": ("Chad", "🇹🇩"), "235": ("Chad", "🇹🇩"),
    "comoros": ("Comoros", "🇰🇲"), "269": ("Comoros", "🇰🇲"),
    "congo": ("Congo", "🇨🇬"), "242": ("Congo", "🇨🇬"),
    "dr congo": ("DR Congo", "🇨🇩"), "243": ("DR Congo", "🇨🇩"),
    "djibouti": ("Djibouti", "🇩🇯"), "253": ("Djibouti", "🇩🇯"),
    "egypt": ("Egypt", "🇪🇬"), "20": ("Egypt", "🇪🇬"),
    "equatorial guinea": ("Equatorial Guinea", "🇬🇶"), "240": ("Equatorial Guinea", "🇬🇶"),
    "eritrea": ("Eritrea", "🇪🇷"), "291": ("Eritrea", "🇪🇷"),
    "eswatini": ("Eswatini", "🇸🇿"), "swaziland": ("Eswatini", "🇸🇿"), "268": ("Eswatini", "🇸🇿"),
    "ethiopia": ("Ethiopia", "🇪🇹"), "251": ("Ethiopia", "🇪🇹"),
    "gabon": ("Gabon", "🇬🇦"), "241": ("Gabon", "🇬🇦"),
    "gambia": ("Gambia", "🇬🇲"), "220": ("Gambia", "🇬🇲"),
    "ghana": ("Ghana", "🇬🇭"), "233": ("Ghana", "🇬🇭"),
    "guinea": ("Guinea", "🇬🇳"), "224": ("Guinea", "🇬🇳"),
    "guinea-bissau": ("Guinea-Bissau", "🇬🇼"), "245": ("Guinea-Bissau", "🇬🇼"),
    "ivory coast": ("Ivory Coast", "🇨🇮"), "cote d'ivoire": ("Ivory Coast", "🇨🇮"), "225": ("Ivory Coast", "🇨🇮"),
    "kenya": ("Kenya", "🇰🇪"), "254": ("Kenya", "🇰🇪"),
    "lesotho": ("Lesotho", "🇱🇸"), "266": ("Lesotho", "🇱🇸"),
    "liberia": ("Liberia", "🇱🇷"), "231": ("Liberia", "🇱🇷"),
    "libya": ("Libya", "🇱🇾"), "218": ("Libya", "🇱🇾"),
    "madagascar": ("Madagascar", "🇲🇬"), "261": ("Madagascar", "🇲🇬"),
    "malawi": ("Malawi", "🇲🇼"), "265": ("Malawi", "🇲🇼"),
    "mali": ("Mali", "🇲🇱"), "223": ("Mali", "🇲🇱"),
    "mauritania": ("Mauritania", "🇲🇷"), "222": ("Mauritania", "🇲🇷"),
    "mauritius": ("Mauritius", "🇲🇺"), "230": ("Mauritius", "🇲🇺"),
    "morocco": ("Morocco", "🇲🇦"), "212": ("Morocco", "🇲🇦"),
    "mozambique": ("Mozambique", "🇲🇿"), "258": ("Mozambique", "🇲🇿"),
    "namibia": ("Namibia", "🇳🇦"), "264": ("Namibia", "🇳🇦"),
    "niger": ("Niger", "🇳🇪"), "227": ("Niger", "🇳🇪"),
    "nigeria": ("Nigeria", "🇳🇬"), "234": ("Nigeria", "🇳🇬"),
    "rwanda": ("Rwanda", "🇷🇼"), "250": ("Rwanda", "🇷🇼"),
    "sao tome": ("Sao Tome and Principe", "🇸🇹"), "239": ("Sao Tome and Principe", "🇸🇹"),
    "senegal": ("Senegal", "🇸🇳"), "221": ("Senegal", "🇸🇳"),
    "seychelles": ("Seychelles", "🇸🇨"), "248": ("Seychelles", "🇸🇨"),
    "sierra leone": ("Sierra Leone", "🇸🇱"), "232": ("Sierra Leone", "🇸🇱"),
    "somalia": ("Somalia", "🇸🇴"), "252": ("Somalia", "🇸🇴"),
    "south africa": ("South Africa", "🇿🇦"), "27": ("South Africa", "🇿🇦"),
    "south sudan": ("South Sudan", "🇸🇸"), "211": ("South Sudan", "🇸🇸"),
    "sudan": ("Sudan", "🇸🇩"), "249": ("Sudan", "🇸🇩"),
    "tanzania": ("Tanzania", "🇹🇿"), "255": ("Tanzania", "🇹🇿"),
    "togo": ("Togo", "🇹🇬"), "228": ("Togo", "🇹🇬"),
    "tunisia": ("Tunisia", "🇹🇳"), "216": ("Tunisia", "🇹🇳"),
    "uganda": ("Uganda", "🇺🇬"), "256": ("Uganda", "🇺🇬"),
    "zambia": ("Zambia", "🇿🇲"), "260": ("Zambia", "🇿🇲"),
    "zimbabwe": ("Zimbabwe", "🇿🇼"), "263": ("Zimbabwe", "🇿🇼"),

    # Asia & Middle East
    "afghanistan": ("Afghanistan", "🇦🇫"), "93": ("Afghanistan", "🇦🇫"),
    "armenia": ("Armenia", "🇦🇲"), "374": ("Armenia", "🇦🇲"),
    "azerbaijan": ("Azerbaijan", "🇦🇿"), "994": ("Azerbaijan", "🇦🇿"),
    "bahrain": ("Bahrain", "🇧🇭"), "973": ("Bahrain", "🇧🇭"),
    "bangladesh": ("Bangladesh", "🇧🇩"), "880": ("Bangladesh", "🇧🇩"),
    "bhutan": ("Bhutan", "🇧🇹"), "975": ("Bhutan", "🇧🇹"),
    "brunei": ("Brunei", "🇧🇳"), "673": ("Brunei", "🇧🇳"),
    "cambodia": ("Cambodia", "🇰🇭"), "855": ("Cambodia", "🇰🇭"),
    "china": ("China", "🇨🇳"), "86": ("China", "🇨🇳"),
    "cyprus": ("Cyprus", "🇨🇾"), "357": ("Cyprus", "🇨🇾"),
    "georgia": ("Georgia", "🇬🇪"), "995": ("Georgia", "🇬🇪"),
    "hong kong": ("Hong Kong", "🇭🇰"), "852": ("Hong Kong", "🇭🇰"),
    "india": ("India", "🇮🇳"), "91": ("India", "🇮🇳"),
    "indonesia": ("Indonesia", "🇮🇩"), "62": ("Indonesia", "🇮🇩"),
    "iran": ("Iran", "🇮🇷"), "98": ("Iran", "🇮🇷"),
    "iraq": ("Iraq", "🇮🇶"), "964": ("Iraq", "🇮🇶"),
    "israel": ("Israel", "🇮🇱"), "972": ("Israel", "🇮🇱"),
    "japan": ("Japan", "🇯🇵"), "81": ("Japan", "🇯🇵"),
    "jordan": ("Jordan", "🇯🇴"), "962": ("Jordan", "🇯🇴"),
    "kazakhstan": ("Kazakhstan", "🇰🇿"), "77": ("Kazakhstan", "🇰🇿"),
    "kuwait": ("Kuwait", "🇰🇼"), "965": ("Kuwait", "🇰🇼"),
    "kyrgyzstan": ("Kyrgyzstan", "🇰🇬"), "996": ("Kyrgyzstan", "🇰🇬"),
    "laos": ("Laos", "🇱🇦"), "856": ("Laos", "🇱🇦"),
    "lebanon": ("Lebanon", "🇱🇧"), "961": ("Lebanon", "🇱🇧"),
    "macau": ("Macau", "🇲🇴"), "853": ("Macau", "🇲🇴"),
    "malaysia": ("Malaysia", "🇲🇾"), "60": ("Malaysia", "🇲🇾"),
    "maldives": ("Maldives", "🇲🇻"), "960": ("Maldives", "🇲🇻"),
    "mongolia": ("Mongolia", "🇲🇳"), "976": ("Mongolia", "🇲🇳"),
    "myanmar": ("Myanmar", "🇲🇲"), "burma": ("Myanmar", "🇲🇲"), "95": ("Myanmar", "🇲🇲"),
    "nepal": ("Nepal", "🇳🇵"), "977": ("Nepal", "🇳🇵"),
    "north korea": ("North Korea", "🇰🇵"), "850": ("North Korea", "🇰🇵"),
    "oman": ("Oman", "🇴🇲"), "968": ("Oman", "🇴🇲"),
    "pakistan": ("Pakistan", "🇵🇰"), "92": ("Pakistan", "🇵🇰"),
    "palestine": ("Palestine", "🇵🇸"), "970": ("Palestine", "🇵🇸"),
    "philippines": ("Philippines", "🇵🇭"), "63": ("Philippines", "🇵🇭"),
    "qatar": ("Qatar", "🇶🇦"), "974": ("Qatar", "🇶🇦"),
    "saudi arabia": ("Saudi Arabia", "🇸🇦"), "966": ("Saudi Arabia", "🇸🇦"),
    "singapore": ("Singapore", "🇸🇬"), "65": ("Singapore", "🇸🇬"),
    "south korea": ("South Korea", "🇰🇷"), "korea": ("South Korea", "🇰🇷"), "82": ("South Korea", "🇰🇷"),
    "sri lanka": ("Sri Lanka", "🇱🇰"), "94": ("Sri Lanka", "🇱🇰"),
    "syria": ("Syria", "🇸🇾"), "963": ("Syria", "🇸🇾"),
    "taiwan": ("Taiwan", "🇹🇼"), "886": ("Taiwan", "🇹🇼"),
    "tajikistan": ("Tajikistan", "🇹🇯"), "992": ("Tajikistan", "🇹🇯"),
    "thailand": ("Thailand", "🇹🇭"), "66": ("Thailand", "🇹🇭"),
    "timor-leste": ("Timor-Leste", "🇹🇱"), "670": ("Timor-Leste", "🇹🇱"),
    "turkey": ("Turkey", "🇹🇷"), "turkiye": ("Turkey", "🇹🇷"), "90": ("Turkey", "🇹🇷"),
    "turkmenistan": ("Turkmenistan", "🇹🇲"), "993": ("Turkmenistan", "🇹🇲"),
    "uae": ("United Arab Emirates", "🇦🇪"), "united arab emirates": ("United Arab Emirates", "🇦🇪"), "971": ("United Arab Emirates", "🇦🇪"),
    "uzbekistan": ("Uzbekistan", "🇺🇿"), "998": ("Uzbekistan", "🇺🇿"),
    "vietnam": ("Vietnam", "🇻🇳"), "84": ("Vietnam", "🇻🇳"),
    "yemen": ("Yemen", "🇾🇪"), "967": ("Yemen", "🇾🇪"),

    # Europe
    "albania": ("Albania", "🇦🇱"), "355": ("Albania", "🇦🇱"),
    "andorra": ("Andorra", "🇦🇩"), "376": ("Andorra", "🇦🇩"),
    "austria": ("Austria", "🇦🇹"), "43": ("Austria", "🇦🇹"),
    "belarus": ("Belarus", "🇧🇾"), "375": ("Belarus", "🇧🇾"),
    "belgium": ("Belgium", "🇧🇪"), "32": ("Belgium", "🇧🇪"),
    "bosnia": ("Bosnia and Herzegovina", "🇧🇦"), "387": ("Bosnia and Herzegovina", "🇧🇦"),
    "bulgaria": ("Bulgaria", "🇧🇬"), "359": ("Bulgaria", "🇧🇬"),
    "croatia": ("Croatia", "🇭🇷"), "385": ("Croatia", "🇭🇷"),
    "czech republic": ("Czech Republic", "🇨🇿"), "czechia": ("Czech Republic", "🇨🇿"), "420": ("Czech Republic", "🇨🇿"),
    "denmark": ("Denmark", "🇩🇰"), "45": ("Denmark", "🇩🇰"),
    "estonia": ("Estonia", "🇪🇪"), "372": ("Estonia", "🇪🇪"),
    "finland": ("Finland", "🇫🇮"), "358": ("Finland", "🇫🇮"),
    "france": ("France", "🇫🇷"), "33": ("France", "🇫🇷"),
    "germany": ("Germany", "🇩🇪"), "49": ("Germany", "🇩🇪"),
    "greece": ("Greece", "🇬🇷"), "30": ("Greece", "🇬🇷"),
    "hungary": ("Hungary", "🇭🇺"), "36": ("Hungary", "🇭🇺"),
    "iceland": ("Iceland", "🇮🇸"), "354": ("Iceland", "🇮🇸"),
    "ireland": ("Ireland", "🇮🇪"), "353": ("Ireland", "🇮🇪"),
    "italy": ("Italy", "🇮🇹"), "39": ("Italy", "🇮🇹"),
    "kosovo": ("Kosovo", "🇽🇰"), "383": ("Kosovo", "🇽🇰"),
    "latvia": ("Latvia", "🇱🇻"), "371": ("Latvia", "🇱🇻"),
    "liechtenstein": ("Liechtenstein", "🇱🇮"), "423": ("Liechtenstein", "🇱🇮"),
    "lithuania": ("Lithuania", "🇱🇹"), "370": ("Lithuania", "🇱🇹"),
    "luxembourg": ("Luxembourg", "🇱🇺"), "352": ("Luxembourg", "🇱🇺"),
    "malta": ("Malta", "🇲🇹"), "356": ("Malta", "🇲🇹"),
    "moldova": ("Moldova", "🇲🇩"), "373": ("Moldova", "🇲🇩"),
    "monaco": ("Monaco", "🇲🇨"), "377": ("Monaco", "🇲🇨"),
    "montenegro": ("Montenegro", "🇲🇪"), "382": ("Montenegro", "🇲🇪"),
    "netherlands": ("Netherlands", "🇳🇱"), "31": ("Netherlands", "🇳🇱"),
    "north macedonia": ("North Macedonia", "🇲🇰"), "389": ("North Macedonia", "🇲🇰"),
    "norway": ("Norway", "🇳🇴"), "47": ("Norway", "🇳🇴"),
    "poland": ("Poland", "🇵🇱"), "48": ("Poland", "🇵🇱"),
    "portugal": ("Portugal", "🇵🇹"), "351": ("Portugal", "🇵🇹"),
    "romania": ("Romania", "🇷🇴"), "40": ("Romania", "🇷🇴"),
    "russia": ("Russia", "🇷🇺"), "7": ("Russia", "🇷🇺"),
    "san marino": ("San Marino", "🇸🇲"), "378": ("San Marino", "🇸🇲"),
    "serbia": ("Serbia", "🇷🇸"), "381": ("Serbia", "🇷🇸"),
    "slovakia": ("Slovakia", "🇸🇰"), "421": ("Slovakia", "🇸🇰"),
    "slovenia": ("Slovenia", "🇸🇮"), "386": ("Slovenia", "🇸🇮"),
    "spain": ("Spain", "🇪🇸"), "34": ("Spain", "🇪🇸"),
    "sweden": ("Sweden", "🇸🇪"), "46": ("Sweden", "🇸🇪"),
    "switzerland": ("Switzerland", "🇨🇭"), "41": ("Switzerland", "🇨🇭"),
    "ukraine": ("Ukraine", "🇺🇦"), "380": ("Ukraine", "🇺🇦"),
    "uk": ("United Kingdom", "🇬🇧"), "united kingdom": ("United Kingdom", "🇬🇧"), "44": ("United Kingdom", "🇬🇧"),
    "vatican": ("Vatican City", "🇻🇦"), "379": ("Vatican City", "🇻🇦"),

    # Americas
    "argentina": ("Argentina", "🇦🇷"), "54": ("Argentina", "🇦🇷"),
    "bahamas": ("Bahamas", "🇧🇸"), "1242": ("Bahamas", "🇧🇸"),
    "barbados": ("Barbados", "🇧🇧"), "1246": ("Barbados", "🇧🇧"),
    "belize": ("Belize", "🇧🇿"), "501": ("Belize", "🇧🇿"),
    "bolivia": ("Bolivia", "🇧🇴"), "591": ("Bolivia", "🇧🇴"),
    "brazil": ("Brazil", "🇧🇷"), "55": ("Brazil", "🇧🇷"),
    "canada": ("Canada", "🇨🇦"), "1": ("Canada", "🇨🇦"),
    "chile": ("Chile", "🇨🇱"), "56": ("Chile", "🇨🇱"),
    "colombia": ("Colombia", "🇨🇴"), "57": ("Colombia", "🇨🇴"),
    "costa rica": ("Costa Rica", "🇨🇷"), "506": ("Costa Rica", "🇨🇷"),
    "cuba": ("Cuba", "🇨🇺"), "53": ("Cuba", "🇨🇺"),
    "dominica": ("Dominica", "🇩🇲"), "1767": ("Dominica", "🇩🇲"),
    "dominican republic": ("Dominican Republic", "🇩🇴"), "1809": ("Dominican Republic", "🇩🇴"),
    "ecuador": ("Ecuador", "🇪🇨"), "593": ("Ecuador", "🇪🇨"),
    "el salvador": ("El Salvador", "🇸🇻"), "503": ("El Salvador", "🇸🇻"),
    "grenada": ("Grenada", "🇬🇩"), "1473": ("Grenada", "🇬🇩"),
    "guatemala": ("Guatemala", "🇬🇹"), "502": ("Guatemala", "🇬🇹"),
    "guyana": ("Guyana", "🇬🇾"), "592": ("Guyana", "🇬🇾"),
    "haiti": ("Haiti", "🇭🇹"), "509": ("Haiti", "🇭🇹"),
    "honduras": ("Honduras", "🇭🇳"), "504": ("Honduras", "🇭🇳"),
    "jamaica": ("Jamaica", "🇯🇲"), "1876": ("Jamaica", "🇯🇲"),
    "mexico": ("Mexico", "🇲🇽"), "52": ("Mexico", "🇲🇽"),
    "nicaragua": ("Nicaragua", "🇳🇮"), "505": ("Nicaragua", "🇳🇮"),
    "panama": ("Panama", "🇵🇦"), "507": ("Panama", "🇵🇦"),
    "paraguay": ("Paraguay", "🇵🇾"), "595": ("Paraguay", "🇵🇾"),
    "peru": ("Peru", "🇵🇪"), "51": ("Peru", "🇵🇪"),
    "suriname": ("Suriname", "🇸🇷"), "597": ("Suriname", "🇸🇷"),
    "trinidad": ("Trinidad and Tobago", "🇹🇹"), "1868": ("Trinidad and Tobago", "🇹🇹"),
    "usa": ("United States", "🇺🇸"), "united states": ("United States", "🇺🇸"), "us": ("United States", "🇺🇸"),
    "uruguay": ("Uruguay", "🇺🇾"), "598": ("Uruguay", "🇺🇾"),
    "venezuela": ("Venezuela", "🇻🇪"), "58": ("Venezuela", "🇻🇪"),

    # Oceania
    "australia": ("Australia", "🇦🇺"), "61": ("Australia", "🇦🇺"),
    "fiji": ("Fiji", "🇫🇯"), "679": ("Fiji", "🇫🇯"),
    "kiribati": ("Kiribati", "🇰🇮"), "686": ("Kiribati", "🇰🇮"),
    "marshall islands": ("Marshall Islands", "🇲🇭"), "692": ("Marshall Islands", "🇲🇭"),
    "micronesia": ("Micronesia", "🇫🇲"), "691": ("Micronesia", "🇫🇲"),
    "nauru": ("Nauru", "🇳🇷"), "674": ("Nauru", "🇳🇷"),
    "new zealand": ("New Zealand", "🇳🇿"), "64": ("New Zealand", "🇳🇿"),
    "palau": ("Palau", "🇵🇼"), "680": ("Palau", "🇵🇼"),
    "papua new guinea": ("Papua New Guinea", "🇵🇬"), "675": ("Papua New Guinea", "🇵🇬"),
    "samoa": ("Samoa", "🇼🇸"), "685": ("Samoa", "🇼🇸"),
    "solomon islands": ("Solomon Islands", "🇸🇧"), "677": ("Solomon Islands", "🇸🇧"),
    "tonga": ("Tonga", "🇹🇴"), "676": ("Tonga", "🇹🇴"),
    "tuvalu": ("Tuvalu", "🇹🇻"), "688": ("Tuvalu", "🇹🇻"),
    "vanuatu": ("Vanuatu", "🇻🇺"), "678": ("Vanuatu", "🇻🇺"),
}


def auto_detect_country_and_flag(country_text: str, phone_or_range: str) -> tuple[str, str]:
    text_clean = (country_text or "").strip().lower()
    digits_only = re.sub(r"\D", "", phone_or_range or "")

    for key, (c_name, flag) in COUNTRY_FLAG_MAP.items():
        if not key.isdigit() and key == text_clean:
            return c_name, flag

    numeric_keys = sorted(
        [k for k in COUNTRY_FLAG_MAP.keys() if k.isdigit()],
        key=len,
        reverse=True
    )
    
    for prefix in numeric_keys:
        if digits_only.startswith(prefix):
            return COUNTRY_FLAG_MAP[prefix]

    return country_text if country_text else "Unknown", "🌐"


def mask_phone_number(phone: str) -> str:
    digits = re.sub(r"\D", "", phone)
    if len(digits) >= 9:
        return f"{digits[:3]}{digits[3:6]}ANU{digits[-2:]}"
    elif len(digits) >= 6:
        return f"{digits[:3]}ANU{digits[-2:]}"
    return phone


def extract_code(message_text: str) -> str:
    match = re.search(r"\b\d{4,8}\b", message_text)
    return match.group(0) if match else "No code found"


# --- API Clients ---

class MKNetworkClient:
    def __init__(self, api_key: str):
        self.base_url = MKNETWORK_BASE_URL
        self.headers = {
            "mknetwork-key": api_key,
            "Content-Type": "application/json",
        }

    async def get_number(self, range_val: str) -> dict:
        async with httpx.AsyncClient() as client:
            try:
                response = await client.post(
                    f"{self.base_url}/getnum/number",
                    headers=self.headers,
                    json={"range": range_val},
                    timeout=10.0,
                )
                return response.json()
            except Exception as e:
                return {"meta": {"code": -500, "status": "error"}, "message": str(e)}

    async def check_status(self, request_ids: list) -> dict:
        async with httpx.AsyncClient() as client:
            try:
                response = await client.post(
                    f"{self.base_url}/check/status",
                    headers=self.headers,
                    json={"request_ids": request_ids},
                    timeout=10.0,
                )
                return response.json()
            except Exception as e:
                return {"meta": {"code": -500, "status": "error"}, "message": str(e)}


class ZebraSMSClient:
    def __init__(self, api_key: str):
        self.base_url = ZEBRA_BASE_URL
        self.headers = {
            "MAuth": api_key,
            "Content-Type": "application/json",
        }

    async def get_number(self, range_val: str) -> dict:
        async with httpx.AsyncClient() as client:
            try:
                response = await client.post(
                    f"{self.base_url}/publicapi/getnum",
                    headers=self.headers,
                    json={"range": range_val},
                    timeout=10.0,
                )
                return response.json()
            except Exception as e:
                return {"meta": {"code": -500, "error": str(e)}}

    async def get_updates(self) -> dict:
        async with httpx.AsyncClient() as client:
            try:
                response = await client.get(
                    f"{self.base_url}/publicapi/getupdate",
                    headers=self.headers,
                    timeout=10.0,
                )
                return response.json()
            except Exception as e:
                return {"meta": {"code": -500, "error": str(e)}}

    async def get_live_access(self, sender: str = None) -> dict:
        params = {"sender": sender} if sender else {}
        async with httpx.AsyncClient() as client:
            try:
                response = await client.get(
                    f"{self.base_url}/publicapi/liveaccess",
                    headers=self.headers,
                    params=params,
                    timeout=10.0,
                )
                return response.json()
            except Exception as e:
                return {"meta": {"code": -500, "error": str(e)}}


mk_client = MKNetworkClient(MKNETWORK_API_KEY) if MKNETWORK_API_KEY else None
zebra = ZebraSMSClient(ZEBRA_API_KEY) if ZEBRA_API_KEY else None


async def cmd_test_sms(update: Update, context: ContextTypes.DEFAULT_TYPE):
    dummy_number = "+23276345652"
    dummy_sender = "FACEBOOK"
    dummy_message = "<#> 031397 is your verification code"
    country_name, flag_icon = auto_detect_country_and_flag("Sierra Leone", dummy_number)

    code = extract_code(dummy_message)
    masked_num = mask_phone_number(dummy_number)

    dm_text = (
        "📩 *[TEST] Verification Code Received!*\n\n"
        f"📱 *To Number:* `{dummy_number}`\n"
        f"👤 *Sender:* `{dummy_sender}`\n"
        f"🔑 *Code:* `{code}`"
    )
    await update.message.reply_text(dm_text, parse_mode="Markdown")

    if CHANNEL_CHAT_ID:
        channel_text = (
            "📢 *New SMS Received*\n\n"
            f"📱 *To Number:* `{masked_num}`\n"
            f"{flag_icon} *Country:* {country_name}\n"
            f"👤 *Sender:* `{dummy_sender}`\n"
            f"💬 *Full Message:* `{dummy_message}`"
        )
        channel_keyboard = InlineKeyboardMarkup([
            [
                InlineKeyboardButton(f"📋 Copy OTP: {code}", callback_data=f"copy_otp_{code}", style="success"),
                InlineKeyboardButton("🤖 Bot", url="https://t.me/anupremiumotp_bot", style="primary")
            ],
            [
                InlineKeyboardButton("👨‍💻 Developer", url="https://t.me/anstans", style="primary")
            ]
        ])
        try:
            await context.bot.send_message(
                chat_id=CHANNEL_CHAT_ID,
                text=channel_text,
                parse_mode="Markdown",
                reply_markup=channel_keyboard,
            )
            await update.message.reply_text("✅ Test message successfully sent to Channel!")
        except Exception as err:
            await update.message.reply_text(f"❌ Failed to send to Channel: `{err}`", parse_mode="Markdown")


async def auto_check_updates(app):
    """Background polling loop supporting both MK Network and Zebra concurrently for active allocations."""
    while True:
        try:
            if active_allocations:
                # 1. Handle MK Network Allocations (stored by request_id)
                mk_req_ids = [req_id for req_id, info in active_allocations.items() if info.get("provider") == "mknetwork"]
                if mk_req_ids and mk_client:
                    for i in range(0, len(mk_req_ids), 100):
                        batch_ids = mk_req_ids[i:i + 100]
                        res = await mk_client.check_status(batch_ids)
                        meta = res.get("meta", {})

                        if meta.get("code") == 200:
                            results = res.get("data", {}).get("results", [])
                            for row in results:
                                req_id = row.get("request_id")
                                status = row.get("status")
                                
                                if req_id not in active_allocations:
                                    continue

                                allocation_info = active_allocations[req_id]
                                user_chat_id = allocation_info["chat_id"]
                                country_name = allocation_info["country"]
                                flag_icon = allocation_info["flag"]
                                target_number = row.get("number")
                                
                                if status == "success":
                                    msg_text = row.get("full_sms", "")
                                    otp_code = row.get("otp_code", "")
                                    sender = row.get("operator", "Other")

                                    msg_id = f"{target_number}_{msg_text}"
                                    if len(seen_messages) > MAX_SEEN_SIZE:
                                        seen_messages.clear()

                                    if msg_id not in seen_messages:
                                        seen_messages.add(msg_id)
                                        
                                        dm_text = (
                                            "📩 *Verification Code Received!*\n\n"
                                            f"📱 *To Number:* `{target_number}`\n"
                                            f"👤 *Sender:* `{sender}`\n"
                                            f"🔑 *Code:* `{otp_code}`"
                                        )
                                        try:
                                            await app.bot.send_message(
                                                chat_id=user_chat_id,
                                                text=dm_text,
                                                parse_mode="Markdown",
                                            )
                                        except Exception as e:
                                            logging.error(f"Error sending DM to {user_chat_id}: {e}")

                                        if CHANNEL_CHAT_ID:
                                            masked_num = mask_phone_number(target_number)
                                            channel_text = (
                                                "📢 *New SMS Received*\n\n"
                                                f"📱 *To Number:* `{masked_num}`\n"
                                                f"{flag_icon} *Country:* {country_name}\n"
                                                f"👤 *Sender:* `{sender}`\n"
                                                f"💬 *Full Message:* `{msg_text}`"
                                            )
                                            channel_keyboard = InlineKeyboardMarkup([
                                                [
                                                    InlineKeyboardButton(f"📋 Copy OTP: {otp_code}", callback_data=f"copy_otp_{otp_code}", style="success"),
                                                    InlineKeyboardButton("🤖 Bot", url="https://t.me/anupremiumotp_bot", style="primary")
                                                ],
                                                [
                                                    InlineKeyboardButton("👨‍💻 Developer", url="https://t.me/anstans", style="primary")
                                                ]
                                            ])
                                            try:
                                                await app.bot.send_message(
                                                    chat_id=CHANNEL_CHAT_ID,
                                                    text=channel_text,
                                                    parse_mode="Markdown",
                                                    reply_markup=channel_keyboard,
                                                )
                                            except Exception as err:
                                                logging.error(f"Failed to post to channel: {err}")

                                    active_allocations.pop(req_id, None)

                                elif status in ["expired", "not_found"]:
                                    active_allocations.pop(req_id, None)

                # 2. Handle Zebra Allocations (stored by number)
                zebra_numbers = [num for num, info in active_allocations.items() if info.get("provider") == "zebra"]
                if zebra_numbers and zebra:
                    res = await zebra.get_updates()
                    meta = res.get("meta", {})

                    if meta.get("code") == 0:
                        rows = res.get("data", {}).get("rows", [])
                        for row in rows:
                            target_number = row.get("number")
                            msg_text = row.get("message")
                            sender = row.get("sender")

                            msg_id = f"{target_number}_{msg_text}"
                            if len(seen_messages) > MAX_SEEN_SIZE:
                                seen_messages.clear()

                            if msg_id not in seen_messages and target_number in active_allocations:
                                seen_messages.add(msg_id)
                                allocation_info = active_allocations[target_number]
                                user_chat_id = allocation_info["chat_id"]
                                country_name = allocation_info["country"]
                                flag_icon = allocation_info["flag"]
                                
                                code = extract_code(msg_text)
                                masked_num = mask_phone_number(target_number)

                                dm_text = (
                                    "📩 *Verification Code Received!*\n\n"
                                    f"📱 *To Number:* `{target_number}`\n"
                                    f"👤 *Sender:* `{sender}`\n"
                                    f"🔑 *Code:* `{code}`"
                                )
                                try:
                                    await app.bot.send_message(
                                        chat_id=user_chat_id,
                                        text=dm_text,
                                        parse_mode="Markdown",
                                    )
                                except Exception as e:
                                    logging.error(f"Error sending DM to {user_chat_id}: {e}")

                                if CHANNEL_CHAT_ID:
                                    channel_text = (
                                        "📢 *New SMS Received*\n\n"
                                        f"📱 *To Number:* `{masked_num}`\n"
                                        f"{flag_icon} *Country:* {country_name}\n"
                                        f"👤 *Sender:* `{sender}`\n"
                                        f"💬 *Full Message:* `{msg_text}`"
                                    )
                                    channel_keyboard = InlineKeyboardMarkup([
                                        [
                                            InlineKeyboardButton(f"📋 Copy OTP: {code}", callback_data=f"copy_otp_{code}", style="success"),
                                            InlineKeyboardButton("🤖 Bot", url="https://t.me/anupremiumotp_bot", style="primary")
                                        ],
                                        [
                                            InlineKeyboardButton("👨‍💻 Developer", url="https://t.me/anstans", style="primary")
                                        ]
                                    ])
                                    try:
                                        await app.bot.send_message(
                                            chat_id=CHANNEL_CHAT_ID,
                                            text=channel_text,
                                            parse_mode="Markdown",
                                            reply_markup=channel_keyboard,
                                        )
                                    except Exception as err:
                                        logging.error(f"Failed to post to channel: {err}")
        except Exception as e:
            logging.error(f"Error during auto-polling: {e}")

        await asyncio.sleep(3)


async def fake_sms_generator_loop(app):
    """Background simulator loop pulling from database ranges to post fake SMS like real ones."""
    while True:
        try:
            await asyncio.sleep(fake_otp_settings["interval"])
            if fake_otp_settings["enabled"] and CHANNEL_CHAT_ID:
                # Pick randomly from either provider's ranges
                all_ranges = db_get_managed_ranges("mknetwork") + db_get_managed_ranges("zebra")
                if all_ranges:
                    chosen_range = random.choice(all_ranges)
                    
                    app_name = chosen_range["service"].upper()
                    country_name = chosen_range["country"]
                    flag_icon = chosen_range["flag"]
                    range_prefix = re.sub(r"\D", "", chosen_range["range"])[:4]
                    
                    simulated_num = f"+{range_prefix}{''.join([str(random.randint(0, 9)) for _ in range(6)])}"
                    otp = str(random.randint(100000, 999999))
                    msg_text = f"<#> {otp} is your {app_name} verification code."
                    masked_num = mask_phone_number(simulated_num)

                    channel_text = (
                        "📢 *New SMS Received*\n\n"
                        f"📱 *To Number:* `{masked_num}`\n"
                        f"{flag_icon} *Country:* {country_name}\n"
                        f"👤 *Sender:* `{app_name}`\n"
                        f"💬 *Full Message:* `{msg_text}`"
                    )
                    channel_keyboard = InlineKeyboardMarkup([
                        [
                            InlineKeyboardButton(f"📋 Copy OTP: {otp}", callback_data=f"copy_otp_{otp}", style="success"),
                            InlineKeyboardButton("🤖 Bot", url="https://t.me/anupremiumotp_bot", style="primary")
                        ],
                        [
                            InlineKeyboardButton("👨‍💻 Developer", url="https://t.me/anstans", style="primary")
                        ]
                    ])
                    try:
                        await app.bot.send_message(
                            chat_id=CHANNEL_CHAT_ID,
                            text=channel_text,
                            parse_mode="Markdown",
                            reply_markup=channel_keyboard,
                        )
                    except Exception as err:
                        logging.error(f"Failed to post simulated SMS to channel: {err}")
        except Exception as e:
            logging.error(f"Error in fake SMS loop: {e}")


def get_main_keyboard():
    keyboard = [
        [KeyboardButton("📱 Get Number 🟢", style="success"), KeyboardButton("⚡ Active Engine ⚡", style="primary")],
        [KeyboardButton("🌐 Live Feed 🔵", style="primary"), KeyboardButton("🎁 Referrals 🟡", style="success")],
        [KeyboardButton("🔐 2FA Online 🟣", style="primary"), KeyboardButton("🎧 Support Hub 🔴", style="danger")],
    ]
    return ReplyKeyboardMarkup(keyboard, resize_keyboard=True)


async def cmd_admin(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id

    if ADMIN_ID != 0 and user_id != ADMIN_ID:
        await update.message.reply_text(f"⛔ **Access Denied:** Your Telegram ID `{user_id}` is not configured as admin.", parse_mode="Markdown")
        return

    keyboard = [
        [
            InlineKeyboardButton("🟢 MK Network Ranges", callback_data="admin_prov_mknetwork", style="success"),
            InlineKeyboardButton("🦓 Zebra SMS Ranges", callback_data="admin_prov_zebra", style="primary"),
        ],
        [
            InlineKeyboardButton("🧪 Toggle Fake SMS 🟠", callback_data="admin_toggle_fake", style="primary"),
            InlineKeyboardButton("📢 Broadcast Msg 🔵", callback_data="admin_broadcast", style="primary"),
        ]
    ]

    fake_status = "🟢 ON" if fake_otp_settings["enabled"] else "🔴 OFF"
    admin_msg = (
        f"🛠 **Admin Configuration Panel** 🛠\n\n"
        f"🧪 **Fake SMS Simulation:** {fake_status} (Interval: {fake_otp_settings['interval']}s)\n\n"
        f"👇 *Select a provider to manage ranges or toggle settings:*"
    )

    await update.message.reply_text(
        admin_msg, parse_mode="Markdown", reply_markup=InlineKeyboardMarkup(keyboard)
    )


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
            [
                f"• {r['flag']} 🔹 **[{r['service']}]** {r['country']} (`{r['range']}`)"
                for r in managed_ranges
            ]
        ) or "No active ranges configured."

        msg = (
            f"🛠 **Admin Panel: {prov.upper()}** 🛠\n\n"
            f"📋 **Current Active Ranges:**\n{ranges_text}\n\n"
            f"👇 *Manage ranges below:*"
        )
        keyboard = [
            [
                InlineKeyboardButton("➕ Add Range 🟢", callback_data=f"admin_add_{prov}", style="success"),
                InlineKeyboardButton("🗑 Delete Range 🔴", callback_data=f"admin_del_select_{prov}", style="danger"),
            ],
            [
                InlineKeyboardButton("⚠️ Clear All Ranges 🟠", callback_data=f"admin_clear_{prov}", style="danger"),
                InlineKeyboardButton("🔙 Back", callback_data="admin_back_main", style="primary")
            ]
        ]
        await query.edit_message_text(msg, parse_mode="Markdown", reply_markup=InlineKeyboardMarkup(keyboard))
        return

    elif data == "admin_back_main":
        await query.answer()
        keyboard = [
            [
                InlineKeyboardButton("🟢 MK Network Ranges", callback_data="admin_prov_mknetwork", style="success"),
                InlineKeyboardButton("🦓 Zebra SMS Ranges", callback_data="admin_prov_zebra", style="primary"),
            ],
            [
                InlineKeyboardButton("🧪 Toggle Fake SMS 🟠", callback_data="admin_toggle_fake", style="primary"),
                InlineKeyboardButton("📢 Broadcast Msg 🔵", callback_data="admin_broadcast", style="primary"),
            ]
        ]
        fake_status = "🟢 ON" if fake_otp_settings["enabled"] else "🔴 OFF"
        admin_msg = (
            f"🛠 **Admin Configuration Panel** 🛠\n\n"
            f"🧪 **Fake SMS Simulation:** {fake_status} (Interval: {fake_otp_settings['interval']}s)\n\n"
            f"👇 *Select a provider to manage ranges or toggle settings:*"
        )
        await query.edit_message_text(admin_msg, parse_mode="Markdown", reply_markup=InlineKeyboardMarkup(keyboard))
        return

    elif data.startswith("admin_add_"):
        prov = data.replace("admin_add_", "")
        context.user_data["waiting_for_range"] = prov
        context.user_data["waiting_for_broadcast"] = False
        example_range = "23276345XXX" if prov == "mknetwork" else "85531879XXX"
        await query.message.reply_text(
            f"✍ **Send the configuration for {prov.upper()} in this format:**\n\n"
            "`Service | Country | Range`\n\n"
            f"👉 *Example:* `Facebook | Sierra Leone | {example_range}`",
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
            [
                InlineKeyboardButton(
                    f"❌ Delete {r['flag']} {r['service']} ({r['range']}) 🔴",
                    callback_data=f"admin_del_{r['range']}",
                    style="danger"
                )
            ]
            for r in managed_ranges
        ]
        keyboard.append([InlineKeyboardButton("🔙 Back", callback_data=f"admin_prov_{prov}", style="primary")])

        await query.edit_message_text(
            f"🗑 **Select a specific {prov.upper()} range to delete:**",
            reply_markup=InlineKeyboardMarkup(keyboard)
        )

    elif data.startswith("admin_del_"):
        range_to_del = data.replace("admin_del_", "")
        db_delete_specific_range(range_to_del)
        await query.answer(f"Deleted range {range_to_del}", show_alert=True)
        await query.edit_message_text(f"✅ **Range `{range_to_del}` deleted successfully.**", parse_mode="Markdown")

    elif data == "admin_toggle_fake":
        fake_otp_settings["enabled"] = not fake_otp_settings["enabled"]
        status_text = "Enabled 🟢" if fake_otp_settings["enabled"] else "Disabled 🔴"
        await query.answer(f"Fake SMS Simulator is now {status_text}", show_alert=True)
        await query.edit_message_text(f"🧪 **Fake SMS Simulation Status:** {status_text}")

    elif data == "admin_broadcast":
        await query.answer()
        context.user_data["waiting_for_broadcast"] = True
        context.user_data["waiting_for_range"] = None
        await query.message.reply_text(
            "📢 **Send the message (Text, Photo, Video, Audio, Document, or Voice) you wish to broadcast to all bot users:**",
            parse_mode="Markdown"
        )

    elif data.startswith("admin_clear_"):
        prov = data.replace("admin_clear_", "")
        db_clear_managed_ranges(prov)
        await query.answer(f"All {prov.upper()} ranges cleared from database!", show_alert=True)
        await query.edit_message_text(f"🗑 **All configured {prov.upper()} ranges have been cleared from database.**")


async def user_provision_callback_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    data = query.data
    chat_id = query.message.chat_id

    if data.startswith("provider_"):
        await query.answer()
        chosen_provider = data.replace("provider_", "")
        managed_ranges = db_get_managed_ranges(chosen_provider)

        if not managed_ranges:
            await query.edit_message_text(
                f"⚠ No ranges configured for `{chosen_provider.upper()}` yet. Add ranges via `/admin`.",
                parse_mode="Markdown",
            )
            return

        services = sorted(list(set(r["service"] for r in managed_ranges)))
        keyboard = [
            [InlineKeyboardButton(f"🛡️ {srv} 🟢", callback_data=f"srv_{chosen_provider}_{srv}", style="success")]
            for srv in services
        ]
        keyboard.append([InlineKeyboardButton("🔙 Back to Providers", callback_data="back_to_providers")])

        await query.edit_message_text(
            f"🛠 *Selected Provider:* `{chosen_provider.upper()}`\n\n👇 *Select a Service:*",
            parse_mode="Markdown",
            reply_markup=InlineKeyboardMarkup(keyboard),
        )
        return

    elif data == "back_to_providers":
        await query.answer()
        keyboard = [
            [
                InlineKeyboardButton("🟢 MK Network", callback_data="provider_mknetwork", style="success"),
                InlineKeyboardButton("🦓 Zebra SMS", callback_data="provider_zebra", style="primary")
            ]
        ]
        await query.edit_message_text(
            "🌐 *Select Provider Gateway:*",
            parse_mode="Markdown",
            reply_markup=InlineKeyboardMarkup(keyboard),
        )
        return

    elif data.startswith("srv_"):
        await query.answer()
        parts = data.replace("srv_", "").split("_", 1)
        chosen_provider = parts[0]
        selected_service = parts[1]

        managed_ranges = db_get_managed_ranges(chosen_provider)
        matching_ranges = [r for r in managed_ranges if r["service"] == selected_service]

        keyboard = [
            [
                InlineKeyboardButton(
                    f"{r['flag']} {r['country']} ({r['range']}) 🔵",
                    callback_data=f"prov_{chosen_provider}_{r['range']}",
                    style="primary"
                )
            ]
            for r in matching_ranges
        ]
        keyboard.append([InlineKeyboardButton("🔙 Back", callback_data=f"provider_{chosen_provider}")])

        await query.edit_message_text(
            f"🛠 *Provider:* `{chosen_provider.upper()}` | *Service:* `{selected_service}`\n\n👇 *Select Country / Range to allocate your number:*",
            parse_mode="Markdown",
            reply_markup=InlineKeyboardMarkup(keyboard),
        )

    elif data.startswith("prov_") or data.startswith("change_"):
        await query.answer()
        is_change = data.startswith("change_")
        
        if is_change:
            clean_data = data.replace("change_", "")
        else:
            clean_data = data.replace("prov_", "")

        parts = clean_data.split("_", 1)
        chosen_provider = parts[0]
        selected_range = parts[1]

        managed_ranges = db_get_managed_ranges(chosen_provider)
        matched_item = next((r for r in managed_ranges if r["range"] == selected_range), {})
        
        c_name, flag_icon = auto_detect_country_and_flag(
            matched_item.get("country", ""), selected_range
        )

        if is_change:
            try:
                await query.message.delete()
            except Exception as e:
                logging.warning(f"Could not delete old message: {e}")

            loading_msg = await context.bot.send_message(
                chat_id=chat_id,
                text=f"⏳ Requesting new number for {flag_icon} range `{selected_range}`...",
                parse_mode="Markdown",
            )
        else:
            await query.edit_message_text(
                f"⏳ Requesting number for {flag_icon} range `{selected_range}`..."
            )

        if chosen_provider == "mknetwork" and mk_client:
            res = await mk_client.get_number(selected_range)
            meta = res.get("meta", {})
            success_cond = (meta.get("code") == 200)
        elif chosen_provider == "zebra" and zebra:
            res = await zebra.get_number(selected_range)
            meta = res.get("meta", {})
            success_cond = (meta.get("code") == 0)
        else:
            success_cond = False
            res = {"meta": {"error": "Client not initialized"}}

        if success_cond:
            if chosen_provider == "mknetwork":
                data_dict = res.get("data", {})
                request_id = data_dict.get("request_id")
                allocated_num = data_dict.get("number")
                api_country = data_dict.get("country")
                allocation_key = request_id
            else:
                row = res["data"]["rows"][0]
                allocated_num = row.get("number")
                api_country = row.get("country")
                allocation_key = allocated_num

            target_country = api_country if api_country else matched_item.get("country", c_name)
            final_country_name, final_flag = auto_detect_country_and_flag(target_country, allocated_num)

            active_allocations[allocation_key] = {
                "chat_id": chat_id,
                "country": final_country_name,
                "flag": final_flag,
                "number": allocated_num,
                "provider": chosen_provider,
            }

            msg = (
                f"✅ **Number Allocated Successfully!**\n\n"
                f"📱 **Number:** `{allocated_num}`\n"
                f"{final_flag} **Country:** {final_country_name}\n"
                f"⏱ **Expires In:** ~15 mins"
            )

            keyboard = InlineKeyboardMarkup(
                [
                    [InlineKeyboardButton("🔄 Change Number 🟠", callback_data=f"change_{chosen_provider}_{selected_range}", style="danger")],
                    [InlineKeyboardButton("📢 Open OTP Channel 🔵", url=CHANNEL_URL, style="primary")],
                ]
            )

            if is_change:
                await loading_msg.edit_text(
                    msg, parse_mode="Markdown", reply_markup=keyboard
                )
            else:
                await query.edit_message_text(
                    msg, parse_mode="Markdown", reply_markup=keyboard
                )
        else:
            err_msg = res.get("message") or meta.get("error") or meta.get("status") or "Unknown error"
            msg = f"❌ **Failed to allocate number:**\nStatus Code: {meta.get('code')}\nDetails: {err_msg}"
            
            if is_change:
                await loading_msg.edit_text(msg, parse_mode="Markdown")
            else:
                await query.edit_message_text(msg, parse_mode="Markdown")

    elif data.startswith("refresh_2fa_"):
        secret = data.replace("refresh_2fa_", "")
        await query.answer("Refreshing 2FA code...")
        try:
            totp = pyotp.TOTP(secret)
            code = totp.now()
            remaining = 30 - (int(asyncio.get_event_loop().time()) % 30)
            
            refresh_keyboard = InlineKeyboardMarkup([
                [InlineKeyboardButton("🔄 Refresh Code", callback_data=f"refresh_2fa_{secret}", style="success")]
            ])
            
            await query.edit_message_text(
                f"🔐 *Generated 2FA Code*\n\n🔑 Code: `{code}`\n⏳ Valid for ~{remaining}s",
                parse_mode="Markdown",
                reply_markup=refresh_keyboard
            )
        except Exception:
            await query.edit_message_text("❌ Failed to refresh code. Secret key may be invalid.")

    elif data.startswith("copy_otp_"):
        otp_val = data.replace("copy_otp_", "")
        await query.answer(f"OTP Copied: {otp_val}", show_alert=True)


async def start_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    user_name = update.effective_user.first_name or "User"
    
    db_add_user(user_id)
    
    welcome_msg = (
        f"👋 *ANU PREMIUM OTP BOT*\n\n"
        f"Welcome, *{user_name}*! 👋\n\n"
        f"Need help or want to add a working number? Contact support: {SUPPORT_USERNAME}"
    )
    await update.message.reply_text(
        welcome_msg, parse_mode="Markdown", reply_markup=get_main_keyboard()
    )


async def handle_text_messages(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_chat_id = update.effective_chat.id
    db_add_user(user_chat_id)

    if context.user_data.get("waiting_for_broadcast"):
        context.user_data["waiting_for_broadcast"] = False
        all_users = db_get_all_users()
        
        if not all_users:
            all_users = [user_chat_id]

        status_msg = await update.message.reply_text(f"⏳ Sending broadcast to {len(all_users)} users...")
        
        success, failed = 0, 0
        for uid in all_users:
            try:
                if update.message.photo:
                    file_id = update.message.photo[-1].file_id
                    caption = update.message.caption_html or update.message.caption or ""
                    await context.bot.send_photo(chat_id=uid, photo=file_id, caption=caption, parse_mode="HTML")
                elif update.message.video:
                    file_id = update.message.video.file_id
                    caption = update.message.caption_html or update.message.caption or ""
                    await context.bot.send_video(chat_id=uid, video=file_id, caption=caption, parse_mode="HTML")
                elif update.message.audio:
                    file_id = update.message.audio.file_id
                    caption = update.message.caption_html or update.message.caption or ""
                    await context.bot.send_audio(chat_id=uid, audio=file_id, caption=caption, parse_mode="HTML")
                elif update.message.voice:
                    file_id = update.message.voice.file_id
                    caption = update.message.caption_html or update.message.caption or ""
                    await context.bot.send_voice(chat_id=uid, voice=file_id, caption=caption, parse_mode="HTML")
                elif update.message.document:
                    file_id = update.message.document.file_id
                    caption = update.message.caption_html or update.message.caption or ""
                    await context.bot.send_document(chat_id=uid, document=file_id, caption=caption, parse_mode="HTML")
                else:
                    text_content = update.message.text_html or update.message.text or ""
                    await context.bot.send_message(
                        chat_id=uid,
                        text=f"📢 **ANNOUNCEMENT** 📢\n\n{text_content}",
                        parse_mode="Markdown"
                    )
                success += 1
            except Exception as e:
                failed += 1
                logging.error(f"Failed to broadcast to {uid}: {e}")

        await status_msg.edit_text(f"✅ **Broadcast Completed!**\n\n Successful: `{success}`\n❌ Failed: `{failed}`", parse_mode="Markdown")
        return

    text = update.message.text.strip() if update.message.text else ""

    if context.user_data.get("waiting_for_2fa"):
        context.user_data["waiting_for_2fa"] = False
        secret_key = text.strip().replace(" ", "")
        try:
            totp = pyotp.TOTP(secret_key)
            code = totp.now()
            remaining = 30 - (int(asyncio.get_event_loop().time()) % 30)
            
            refresh_keyboard = InlineKeyboardMarkup([
                [InlineKeyboardButton("🔄 Refresh Code", callback_data=f"refresh_2fa_{secret_key}", style="success")]
            ])
            
            await update.message.reply_text(
                f"🔐 *Generated 2FA Code*\n\n🔑 Code: `{code}`\n⏳ Valid for ~{remaining}s",
                parse_mode="Markdown",
                reply_markup=refresh_keyboard
            )
        except Exception:
            await update.message.reply_text("❌ Invalid 2FA secret key! Try again using the '2FA Online' button.", reply_markup=get_main_keyboard())
        return

    if context.user_data.get("waiting_for_range"):
        prov = context.user_data.get("waiting_for_range")
        if text.count("|") != 2:
            example_range = "23276345XXX" if prov == "mknetwork" else "85531879XXX"
            await update.message.reply_text(
                "⚠️ **Format Error!** Use: `Service | Country | Range`\n"
                f"Example: `Facebook | Sierra Leone | {example_range}`",
                parse_mode="Markdown",
            )
            return

        parts = [p.strip() for p in text.split("|")]
        service, country, range_val = parts[0], parts[1], parts[2]
        c_name, detected_flag = auto_detect_country_and_flag(country, range_val)

        db_add_managed_range(service, c_name, detected_flag, range_val, provider=prov)
        
        context.user_data["waiting_for_range"] = None

        await update.message.reply_text(
            f"✅ **Successfully saved range to database for {prov.upper()}!**\n"
            f"📌 **Service:** `{service}`\n"
            f"{detected_flag} **Country:** `{c_name}`\n"
            f"🔢 **Range:** `{range_val}`",
            parse_mode="Markdown",
            reply_markup=get_main_keyboard(),
        )
        return

    if "Get Number" in text:
        keyboard = [
            [
                InlineKeyboardButton("🟢 ANU 🥺", callback_data="provider_mknetwork", style="success"),
                InlineKeyboardButton("🦓 ANU 🤡", callback_data="provider_zebra", style="primary")
            ]
        ]
        await update.message.reply_text(
            "🌐 *Select Provider Gateway:*",
            parse_mode="Markdown",
            reply_markup=InlineKeyboardMarkup(keyboard),
        )

    elif "Active Engine" in text:
        if zebra:
            await update.message.reply_text("⏳ Fetching active delivery engines...")
            res = await zebra.get_live_access()
            meta = res.get("meta", {})

            if meta.get("code") == 0:
                rows = res.get("data", {}).get("rows", [])
                reply = "⚡ **Zebra Active Delivery Engines:**\n\n" + "\n".join(
                    [f"• 👤 **Sender:** `{r.get('sender')}` | **Ranges:** {', '.join([f'`{x}`' for x in r.get('ranges', [])])}" for r in rows[:10]]
                ) if rows else "🔍 No active engines found right now."
            else:
                reply = f"❌ Error checking active engines: {meta.get('error')}"
            await update.message.reply_text(reply, parse_mode="Markdown")
        else:
            await update.message.reply_text("⚡ Active delivery engines are online and ready.")

    elif "Live Feed" in text:
        user_mk_reqs = [req_id for req_id, info in active_allocations.items() if info.get("chat_id") == user_chat_id and info.get("provider") == "mknetwork"]
        
        mk_results_text = ""
        if user_mk_reqs and mk_client:
            res = await mk_client.check_status(user_mk_reqs)
            meta = res.get("meta", {})
            if meta.get("code") == 200:
                results = res.get("data", {}).get("results", [])
                if results:
                    mk_results_text = "🌐 **MK Network Active Status:**\n\n" + "\n".join(
                        [f"• 📱 `{mask_phone_number(r.get('number'))}` | 📌 Status: `{r.get('status')}`\n  🔑 Code: `{r.get('otp_code') or 'Waiting...'}`" for r in results]
                    )

        zebra_results_text = ""
        if zebra:
            res = await zebra.get_updates()
            meta = res.get("meta", {})
            if meta.get("code") == 0:
                rows = res.get("data", {}).get("rows", [])
                if rows:
                    zebra_results_text = "🦓 **Zebra Recent Updates:**\n\n" + "\n".join(
                        [f"• 📱 `{mask_phone_number(r.get('number'))}` | 👤 `{r.get('sender')}`\n  💬 `{r.get('message')}`" for r in rows[:3]]
                    )

        combined_reply = "\n\n".join([t for t in [mk_results_text, zebra_results_text] if t])
        if not combined_reply:
            combined_reply = "📭 No active numbers or live updates found."

        await update.message.reply_text(combined_reply, parse_mode="Markdown")

    elif "Referrals" in text:
        bot_username = (await context.bot.get_me()).username
        await update.message.reply_text(
            f"🎁 **Referral System**\n\n"
            f"Share your referral link with friends:\n🔗 `https://t.me/{bot_username}?start={user_chat_id}`",
            parse_mode="Markdown",
        )

    elif "My Profile" in text:
        user_nums = [info.get("number") for n, info in active_allocations.items() if info["chat_id"] == user_chat_id]
        nums_text = "\n".join([f"• `{num}`" for num in user_nums]) if user_nums else "None"
        await update.message.reply_text(
            f"👤 **User Profile**\n\n"
            f"🆔 **Telegram ID:** `{user_chat_id}`\n"
            f"📱 **Active Numbers:**\n{nums_text}",
            parse_mode="Markdown",
        )

    elif "2FA Online" in text:
        context.user_data["waiting_for_2fa"] = True
        await update.message.reply_text("🔐 **Send your 2FA Secret Key below:**", parse_mode="Markdown")

    elif "Support Hub" in text:
        await update.message.reply_text(
            f"🎧 **Support Hub**\n\nContact support agent directly {SUPPORT_USERNAME}",
            parse_mode="Markdown",
        )


async def post_init(application):
    application.create_task(auto_check_updates(application))
    application.create_task(fake_sms_generator_loop(application))


# --- Main Execution ---

if __name__ == "__main__":
    keep_alive()
    print("Keep-alive HTTP server started.")

    app = (
        ApplicationBuilder()
        .token(TELEGRAM_BOT_TOKEN)
        .post_init(post_init)
        .build()
    )

    app.add_handler(CommandHandler("start", start_command))
    app.add_handler(CommandHandler("admin", cmd_admin))
    app.add_handler(CommandHandler("test_sms", cmd_test_sms))

    app.add_handler(CallbackQueryHandler(admin_callback_handler, pattern="^admin_"))
    app.add_handler(CallbackQueryHandler(user_provision_callback_handler, pattern="^(provider_|back_to_providers|srv_|prov_|change_|refresh_2fa_|copy_otp_)"))

    app.add_handler(MessageHandler(filters.ALL & ~filters.COMMAND, handle_text_messages))

    print("🤖 Bot running with combined MK Network and Zebra API integrations...")
    app.run_polling()
