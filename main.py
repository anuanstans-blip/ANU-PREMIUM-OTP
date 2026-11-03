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

# Provider 1: SL Virtual
SLV_API_KEY = os.getenv("SLV_API_KEY", "").strip(" '\"")
SLV_BASE_URL = os.getenv("SLV_BASE_URL", "https://slvirtualnumber.com").strip(" '\"")

# Provider 2: ZebraSMS
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

DEFAULT_RANGES = [
    {"service": "WhatsApp", "country": "Italy", "flag": "🇮🇹", "range": "39324", "tag": "SLVN-ITA-1", "provider": "slvirtual"},
    {"service": "WhatsApp", "country": "Ivory Coast", "flag": "🇨🇮", "range": "22501XXX", "tag": "ZEBRA-CI-1", "provider": "zebrasms"},
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
        return [r for r in DEFAULT_RANGES if r["provider"] == provider]
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
        return DEFAULT_RANGES
    try:
        response = supabase.table("managed_ranges").select("*").execute()
        return response.data if response.data else []
    except Exception as e:
        logging.error(f"Error fetching all ranges from Supabase: {e}")
        return []


def db_add_managed_range(service: str, country: str, flag: str, range_val: str, tag: str, provider: str):
    if not supabase:
        return
    try:
        data = {
            "service": service,
            "country": country,
            "flag": flag,
            "range": range_val,
            "tag": tag,
            "provider": provider,
        }
        supabase.table("managed_ranges").upsert(data, on_conflict="service, range, tag").execute()
    except Exception as e:
        logging.error(f"Error inserting range to Supabase: {e}")


def db_delete_specific_range(range_val: str, service: str = None, tag: str = None):
    if not supabase:
        return
    try:
        query = supabase.table("managed_ranges").delete().eq("range", range_val)
        if service:
            query = query.eq("service", service)
        if tag:
            query = query.eq("tag", tag)
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


# --- Full World Country & Flag Map ---
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

    numeric_keys = sorted([k for k in COUNTRY_FLAG_MAP.keys() if k.isdigit()], key=len, reverse=True)
    for prefix in numeric_keys:
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


# --- API Clients for Both Providers ---

class SLVirtualNumberClient:
    def __init__(self, api_key: str):
        self.base_url = SLV_BASE_URL.rstrip("/")
        self.headers = {"X-API-KEY": api_key, "Content-Type": "application/json"}
        self.client = httpx.AsyncClient(follow_redirects=True, timeout=5.0)

    async def get_number(self, range_val: str, service: str = "WhatsApp", tag: str = None, country_code: str = None) -> dict:
        try:
            payload = {"rangePrefix": range_val, "service": service}
            if tag:
                payload["tag"] = tag
            if country_code:
                payload["countryCode"] = country_code

            response = await self.client.post(f"{self.base_url}/api/v1/developer/rent", headers=self.headers, json=payload)
            if response.status_code != 200:
                return {"status": "error", "message": f"HTTP {response.status_code}"}
            return response.json()
        except Exception as e:
            return {"status": "error", "message": str(e)}

    async def check_status(self, rental_id: str) -> dict:
        try:
            response = await self.client.get(f"{self.base_url}/api/v1/developer/check-otp/{rental_id}", headers=self.headers)
            if response.status_code != 200:
                return {"status": "error", "message": f"HTTP {response.status_code}"}
            return response.json()
        except Exception as e:
            return {"status": "error", "message": str(e)}


class ZebraSMSClient:
    def __init__(self, api_key: str):
        self.base_url = ZEBRA_BASE_URL.rstrip("/")
        self.headers = {"MAuth": api_key, "Content-Type": "application/json"}
        self.client = httpx.AsyncClient(follow_redirects=True, timeout=5.0)

    async def get_number(self, range_val: str) -> dict:
        try:
            payload = {"range": range_val}
            response = await self.client.post(f"{self.base_url}/publicapi/getnum", headers=self.headers, json=payload)
            if response.status_code != 200:
                return {"meta": {"code": -500}, "message": f"HTTP {response.status_code}"}
            return response.json()
        except Exception as e:
            return {"meta": {"code": -500}, "message": str(e)}

    async def get_updates(self) -> dict:
        try:
            response = await self.client.get(f"{self.base_url}/publicapi/getupdate", headers=self.headers)
            if response.status_code != 200:
                return {"meta": {"code": -500}, "message": f"HTTP {response.status_code}"}
            return response.json()
        except Exception as e:
            return {"meta": {"code": -500}, "message": str(e)}


slv_client = SLVirtualNumberClient(SLV_API_KEY) if SLV_API_KEY else None
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
    await update.message.reply_text("✅ Test command executed successfully!")


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
            tag_name = allocation_info.get("tag", "Default")

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
                    f"🏷 *Batch Tag:* `{tag_name}`\n"
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


# --- Dual Provider Polling Workers ---
async def poll_single_allocation(app, alloc_key):
    start_time = asyncio.get_event_loop().time()

    while alloc_key in active_allocations:
        if asyncio.get_event_loop().time() - start_time > 900:
            active_allocations.pop(alloc_key, None)
            break

        info = active_allocations.get(alloc_key)
        if not info:
            break

        provider = info.get("provider")

        try:
            if provider == "slvirtual" and slv_client:
                res = await slv_client.check_status(alloc_key)
                if res.get("status") == "success":
                    data_row = res.get("data", {})
                    if data_row.get("status") == "RECEIVED":
                        await dispatch_otp(app, info, data_row.get("phoneNumber"), data_row.get("otpCode"), data_row.get("service"), data_row.get("fullSms"))
                        active_allocations.pop(alloc_key, None)
                        return

            elif provider == "zebrasms" and zebra_client:
                res = await zebra_client.get_updates()
                if res.get("meta", {}).get("code") == 0:
                    rows = res.get("data", {}).get("rows", [])
                    for row in rows:
                        if row.get("number") == info["number"]:
                            msg_text = row.get("message", "")
                            otp_code = extract_code(msg_text)
                            if otp_code != "No code found":
                                await dispatch_otp(app, info, info["number"], otp_code, row.get("sender", "SMS"), msg_text)
                                active_allocations.pop(alloc_key, None)
                                return
        except Exception as e:
            logging.error(f"Error polling worker for {alloc_key}: {e}")

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
        await update.message.reply_text(f"⛔ **Access Denied:** Your ID `{user_id}` is not admin.", parse_mode="Markdown")
        return

    keyboard = [
        [
            InlineKeyboardButton("🌐 SL Virtual (Provider 1)", callback_data="admin_prov_slvirtual"),
            InlineKeyboardButton("🦓 ZebraSMS (Provider 2)", callback_data="admin_prov_zebrasms"),
        ]
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
            [f"• {r['flag']} 🔹 **[{r['service']}]** {r['country']} (`{r['range']}`) Tag: `{r.get('tag', 'N/A')}`" for r in managed_ranges]
        ) or "No active ranges configured."

        msg = f"🛠 **Admin Panel: {prov.upper()}**\n\n📋 **Active Ranges:**\n{ranges_text}\n\n👇 *Manage ranges below:*"
        keyboard = [
            [
                InlineKeyboardButton("➕ Add Range 🟢", callback_data=f"admin_add_{prov}"),
                InlineKeyboardButton("🗑 Delete Range 🔴", callback_data=f"admin_del_select_{prov}"),
            ],
            [
                InlineKeyboardButton("⚠️ Clear All 🟠", callback_data=f"admin_clear_{prov}"),
                InlineKeyboardButton("🔙 Back", callback_data="admin_back_main")
            ]
        ]
        await query.edit_message_text(msg, parse_mode="Markdown", reply_markup=InlineKeyboardMarkup(keyboard))
        return

    elif data == "admin_back_main":
        await query.answer()
        keyboard = [
            [
                InlineKeyboardButton("🌐 SL Virtual (Provider 1)", callback_data="admin_prov_slvirtual"),
                InlineKeyboardButton("🦓 ZebraSMS (Provider 2)", callback_data="admin_prov_zebrasms"),
            ]
        ]
        await query.edit_message_text("🛠 **Admin Configuration Panel**\n\n👇 *Select provider to manage ranges:*", parse_mode="Markdown", reply_markup=InlineKeyboardMarkup(keyboard))
        return

    elif data.startswith("admin_add_"):
        prov = data.replace("admin_add_", "")
        context.user_data["waiting_for_range"] = prov
        await query.message.reply_text(f"✍ **Send configuration for {prov}:**\n\n`Service | Country | Range | Tag`\n👉 *Example:* `WhatsApp | Italy | 39324 | SLVN-ITA-1`", parse_mode="Markdown")
        await query.answer()

    elif data.startswith("admin_del_select_"):
        prov = data.replace("admin_del_select_", "")
        await query.answer()
        managed_ranges = db_get_managed_ranges(prov)
        if not managed_ranges:
            await query.edit_message_text("❌ No active ranges found to delete.")
            return

        keyboard = [
            [InlineKeyboardButton(f"❌ Delete {r['flag']} {r['service']} ({r['range']}) [{r.get('tag', '')}]", callback_data=f"admin_del_{prov}_{r['service']}_{r['range']}_{r.get('tag', 'def')}")]
            for r in managed_ranges
        ]
        await query.edit_message_text("🗑 **Select range to delete:**", reply_markup=InlineKeyboardMarkup(keyboard))

    elif data.startswith("admin_del_"):
        parts = data.replace("admin_del_", "").split("_", 3)
        if len(parts) >= 3:
            prov, srv_to_del, range_to_del = parts[0], parts[1], parts[2]
            tag_to_del = parts[3] if len(parts) > 3 and parts[3] != "def" else None
            db_delete_specific_range(range_to_del, service=srv_to_del, tag=tag_to_del)
            await query.answer(f"Deleted range {range_to_del}", show_alert=True)
            await query.edit_message_text(f"✅ **Range `{range_to_del}` for `{srv_to_del}` deleted.**", parse_mode="Markdown")

    elif data.startswith("admin_clear_"):
        prov = data.replace("admin_clear_", "")
        db_clear_managed_ranges(prov)
        await query.answer("All ranges cleared!", show_alert=True)
        await query.edit_message_text(f"🗑 **All configured ranges cleared for {prov}.**")


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
                f"[{r['provider'].upper()}] {r['flag']} {r['country']} ({r.get('tag', r['range'])})", 
                callback_data=f"prov_{r['provider']}_{r['range']}_{r.get('tag', '')}"
            )
            for r in matching_ranges
        ]
        keyboard = [country_buttons[i:i + 2] for i in range(0, len(country_buttons), 2)]
        keyboard.append([InlineKeyboardButton("🔙 Back to Services", callback_data="back_to_services")])

        await query.edit_message_text(
            f"🛠 *Selected Service:* `{selected_service}`\n\n👇 *Select Provider/Country to allocate number:*",
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

        parts = clean_data.split("_", 2)
        chosen_provider = parts[0]
        selected_range = parts[1]
        target_tag = parts[2] if len(parts) > 2 else ""

        if is_change:
            try:
                await query.message.delete()
            except Exception as e:
                logging.error(f"Error deleting message: {e}")

        all_ranges = db_get_all_managed_ranges()
        matched_item = next(
            (r for r in all_ranges if r["range"] == selected_range and r["provider"] == chosen_provider and r.get("tag", "") == target_tag), 
            next((r for r in all_ranges if r["range"] == selected_range and r["provider"] == chosen_provider), {})
        )
        
        c_name, flag_icon = auto_detect_country_and_flag(matched_item.get("country", ""), selected_range)
        stored_tag = matched_item.get("tag", "")

        loading_msg = await context.bot.send_message(
            chat_id=chat_id, 
            text=f"⏳ Requesting fresh number from **{chosen_provider.upper()}** ({flag_icon} `{selected_range}`)...", 
            parse_mode="Markdown"
        )

        alloc_key, allocated_num, final_country = None, None, c_name

        if chosen_provider == "slvirtual" and slv_client:
            await asyncio.sleep(0.3)
            c_code = "ET" if "Ethiopia" in matched_item.get("country", "") else None
            res = await slv_client.get_number(range_val=selected_range, service=matched_item.get("service", "WhatsApp"), tag=stored_tag, country_code=c_code)
            if isinstance(res, dict) and res.get("status") == "success":
                d = res.get("data", {})
                alloc_key, allocated_num, final_country = d.get("rentalId"), d.get("phoneNumber"), d.get("country")

        elif chosen_provider == "zebrasms" and zebra_client:
            await asyncio.sleep(0.3)
            res = await zebra_client.get_number(range_val=selected_range)
            if isinstance(res, dict) and res.get("meta", {}).get("code") == 0:
                rows = res.get("data", {}).get("rows", [])
                if rows:
                    d = rows[0]
                    alloc_key, allocated_num, final_country = d.get("number"), d.get("number"), d.get("country")

        if alloc_key and allocated_num:
            final_country_name, final_flag = auto_detect_country_and_flag(final_country, allocated_num)
            rand_fname, rand_lname = generate_random_english_name()

            active_allocations[alloc_key] = {
                "chat_id": chat_id,
                "country": final_country_name,
                "flag": final_flag,
                "number": allocated_num,
                "provider": chosen_provider,
                "first_name": rand_fname,
                "tag": stored_tag or "Default"
            }

            msg = (
                f"✅ **Allocated via {chosen_provider.upper()}!**\n\n"
                f"👤 **First Name:** `{rand_fname}`\n"
                f"👤 **Last Name:** `{rand_lname}`\n"
                f"{final_flag} **Country:** {final_country_name}\n"
                f"🏷 **Tag:** `{stored_tag or 'Default'}`\n"
                f"⏱ **Expires In:** ~15 mins"
            )

            keyboard = InlineKeyboardMarkup([
                [InlineKeyboardButton(f"📱 {allocated_num}", copy_text=CopyTextButton(text=str(allocated_num)))],
                [InlineKeyboardButton("🔄 Change Number 🟠", callback_data=f"change_{chosen_provider}_{selected_range}_{stored_tag}")],
                [InlineKeyboardButton("📢 Open OTP Channel 🔵", url=CHANNEL_URL)],
            ])

            allocated_msg = await loading_msg.edit_text(msg, parse_mode="Markdown", reply_markup=keyboard)
            asyncio.create_task(auto_delete_allocation_message(context, chat_id, allocated_msg.message_id, alloc_key))
        else:
            fallback_keyboard = InlineKeyboardMarkup([
                [InlineKeyboardButton("🔄 Try Again", callback_data=data)],
                [InlineKeyboardButton("🔙 Back to Services", callback_data="back_to_services")]
            ])
            await loading_msg.edit_text("❌ **Out of stock or API error.** Tap below to try again.", parse_mode="Markdown", reply_markup=fallback_keyboard)

    elif data.startswith("refresh_2fa_"):
        secret = data.replace("refresh_2fa_", "")
        await query.answer("Refreshing 2FA code...")
        try:
            totp = pyotp.TOTP(secret)
            code = totp.now()
            remaining = 30 - (int(asyncio.get_event_loop().time()) % 30)
            refresh_keyboard = InlineKeyboardMarkup([[InlineKeyboardButton("🔄 Refresh Code", callback_data=f"refresh_2fa_{secret}")]])
            await query.edit_message_text(f"🔐 *Generated 2FA Code*\n\n🔑 Code: `{code}`\n⏳ Valid for ~{remaining}s", parse_mode="Markdown", reply_markup=refresh_keyboard)
        except Exception:
            await query.edit_message_text("❌ Invalid 2FA secret key!")


async def start_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    user_name = update.effective_user.first_name or "User"
    db_add_user(user_id)
    
    welcome_msg = (
        f"👋 *ANU PREMIUM MULTI-PROVIDER OTP BOT*\n\n"
        f"Welcome, *{user_name}*! 👋\n\n"
        f"Need help? Contact support: {SUPPORT_USERNAME}"
    )
    await update.message.reply_text(welcome_msg, parse_mode="Markdown", reply_markup=get_main_keyboard())


async def handle_text_messages(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_chat_id = update.effective_chat.id
    db_add_user(user_chat_id)

    text = update.message.text.strip() if update.message.text else ""

    if context.user_data.get("waiting_for_range"):
        prov = context.user_data.get("waiting_for_range")
        if text.count("|") not in [2, 3]:
            await update.message.reply_text("⚠️ **Format Error!** Use: `Service | Country | Range | Tag`", parse_mode="Markdown")
            return

        parts = [p.strip() for p in text.split("|")]
        service = parts[0].upper()
        country = parts[1]
        range_val = parts[2]
        tag_val = parts[3] if len(parts) == 4 else "Default"

        c_name, detected_flag = auto_detect_country_and_flag(country, range_val)
        db_add_managed_range(service, c_name, detected_flag, range_val, tag_val, provider=prov)
        context.user_data["waiting_for_range"] = None

        await update.message.reply_text(
            f"✅ **Successfully saved range for {prov}!**\n📌 Service: `{service}`\n{detected_flag} Country: `{c_name}`\n🔢 Range: `{range_val}`\n🏷 Tag: `{tag_val}`",
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
        await update.message.reply_text("⚡ Multi-provider active engines are online.", parse_mode="Markdown")

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
    app.add_handler(CallbackQueryHandler(user_provision_callback_handler, pattern="^(get_srv_|back_to_services|prov_|change_|refresh_2fa_)"))

    app.add_handler(MessageHandler(filters.ALL & ~filters.COMMAND, handle_text_messages))

    print("🤖 Multi-Provider OTP Bot running successfully...")
    app.run_polling()
