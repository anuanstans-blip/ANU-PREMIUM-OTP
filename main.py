import asyncio
import logging
import os
import random
import re
import httpx
import pandas as pd
from threading import Thread
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

# Fallback default ranges if database is empty
DEFAULT_RANGES = [
    {"service": "Facebook", "country": "Cambodia", "flag": "🇰🇭", "range": "85531879XXX"},
    {"service": "Telegram", "country": "Ivory Coast", "flag": "🇨🇮", "range": "22501XXX"},
]

# --- Load Names from Excel (Non-blocking background initialization) ---
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
EXCEL_PATH = os.path.join(BASE_DIR, "name.xlsx")

MALE_NAMES = []
FEMALE_NAMES = []

def load_names_background():
    global MALE_NAMES, FEMALE_NAMES
    try:
        if os.path.exists(EXCEL_PATH):
            df = pd.read_excel(EXCEL_PATH)
            # Ensure proper string stripping and column name cleaning
            df.columns = [c.strip() for c in df.columns]
            
            if "First Name" in df.columns and "Last Name" in df.columns and "Gender" in df.columns:
                df["Gender"] = df["Gender"].astype(str).str.strip().str.lower()
                df["Full_Name"] = df["First Name"].astype(str).str.strip() + " " + df["Last Name"].astype(str).str.strip()
                
                male_df = df[df["Gender"] == "male"]
                female_df = df[df["Gender"] == "female"]
                
                MALE_NAMES = male_df["Full_Name"].dropna().tolist()
                FEMALE_NAMES = female_df["Full_Name"].dropna().tolist()
                logging.info(f"Loaded {len(MALE_NAMES)} male names and {len(FEMALE_NAMES)} female names successfully from name.xlsx.")
            else:
                logging.error("Excel columns 'First Name', 'Last Name', or 'Gender' not found in name.xlsx")
        else:
            logging.warning(f"Excel file not found at: {EXCEL_PATH}")
    except Exception as e:
        logging.error(f"Failed to load names from Excel: {e}")

# Start background thread immediately so port binding isn't delayed
Thread(target=load_names_background, daemon=True).start()


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


def db_get_managed_ranges() -> list:
    if not supabase:
        return DEFAULT_RANGES
    try:
        response = supabase.table("managed_ranges").select("*").execute()
        if response.data:
            return response.data
        return DEFAULT_RANGES
    except Exception as e:
        logging.error(f"Error fetching ranges from Supabase: {e}")
        return DEFAULT_RANGES


def db_add_managed_range(service: str, country: str, flag: str, range_val: str):
    if not supabase:
        return
    try:
        data = {
            "service": service,
            "country": country,
            "flag": flag,
            "range": range_val,
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


def db_clear_managed_ranges():
    if not supabase:
        return
    try:
        supabase.table("managed_ranges").delete().neq("id", 0).execute()
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
    if len(digits) >= 8:
        prefix = digits[:3]
        suffix = digits[-4:]
        return f"+{prefix}****{suffix}"
    return phone


def extract_code(message_text: str) -> str:
    match = re.search(r"\b\d{4,8}\b", message_text)
    return match.group(0) if match else "No code found"


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


zebra = ZebraSMSClient(ZEBRA_API_KEY)


async def cmd_test_sms(update: Update, context: ContextTypes.DEFAULT_TYPE):
    dummy_number = "+237628503546"
    dummy_sender = "FACEBOOK"
    dummy_message = "<#> 40921 is your Facebook code Laz+nxCarLW"
    country_name, flag_icon = auto_detect_country_and_flag("Cameroon", dummy_number)

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
        try:
            await context.bot.send_message(
                chat_id=CHANNEL_CHAT_ID,
                text=channel_text,
                parse_mode="Markdown",
            )
            await update.message.reply_text("✅ Test message successfully sent to Channel!")
        except Exception as err:
            await update.message.reply_text(f"❌ Failed to send to Channel: `{err}`", parse_mode="Markdown")


async def auto_check_updates(app):
    while True:
        try:
            if active_allocations:
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
                                try:
                                    await app.bot.send_message(
                                        chat_id=CHANNEL_CHAT_ID,
                                        text=channel_text,
                                        parse_mode="Markdown",
                                    )
                                except Exception as err:
                                    logging.error(f"Failed to post to channel: {err}")
        except Exception as e:
            logging.error(f"Error during auto-polling: {e}")

        await asyncio.sleep(2)


def get_main_keyboard():
    keyboard = [
        [KeyboardButton("📱 Get Number 🟢"), KeyboardButton("🏷️ Name Generate 🔤")],
        [KeyboardButton("⚡ Active Engine ⚡"), KeyboardButton("🌐 Live Feed 🔵")],
        [KeyboardButton("🎁 Referrals 🟡"), KeyboardButton("👤 My Profile 🟣")],
        [KeyboardButton("🎧 Support Hub 🔴")],
    ]
    return ReplyKeyboardMarkup(keyboard, resize_keyboard=True)


async def cmd_admin(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id

    if ADMIN_ID != 0 and user_id != ADMIN_ID:
        await update.message.reply_text(f"⛔ **Access Denied:** Your Telegram ID `{user_id}` is not configured as admin.", parse_mode="Markdown")
        return

    managed_ranges = db_get_managed_ranges()

    ranges_text = "\n".join(
        [
            f"• {r['flag']} 🔹 **[{r['service']}]** {r['country']} (`{r['range']}`)"
            for r in managed_ranges
        ]
    ) or "No active ranges configured."

    admin_msg = (
        f"🛠 **Admin Configuration Panel** 🛠\n\n"
        f"📋 **Current Active Ranges (Stored in Database):**\n{ranges_text}\n\n"
        f"👇 *Click below to add, delete, or broadcast messages:*"
    )

    keyboard = [
        [
            InlineKeyboardButton("➕ Add Range 🟢", callback_data="admin_add"),
            InlineKeyboardButton("🗑 Delete Range 🔴", callback_data="admin_delete_select"),
        ],
        [
            InlineKeyboardButton("📢 Broadcast Msg 🔵", callback_data="admin_broadcast"),
            InlineKeyboardButton("⚠️ Clear All Ranges 🟠", callback_data="admin_clear"),
        ]
    ]

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
    if data == "admin_add":
        await query.answer()
        context.user_data["waiting_for_range"] = True
        context.user_data["waiting_for_broadcast"] = False
        await query.message.reply_text(
            "✍ **Send the configuration in this format:**\n\n"
            "`Service | Country | Range`\n\n"
            "👉 *Example:* `Facebook | Cambodia | 85531879XXX`",
            parse_mode="Markdown",
        )
    elif data == "admin_delete_select":
        await query.answer()
        managed_ranges = db_get_managed_ranges()
        if not managed_ranges:
            await query.edit_message_text("❌ No active ranges found to delete.")
            return

        keyboard = [
            [
                InlineKeyboardButton(
                    f"❌ Delete {r['flag']} {r['service']} ({r['range']}) 🔴",
                    callback_data=f"admin_del_{r['range']}"
                )
            ]
            for r in managed_ranges
        ]
        keyboard.append([InlineKeyboardButton("🔙 Back 🟡", callback_data="admin_back")])

        await query.edit_message_text(
            "🗑 **Select a specific range to delete:**",
            reply_markup=InlineKeyboardMarkup(keyboard)
        )

    elif data.startswith("admin_del_"):
        range_to_del = data.replace("admin_del_", "")
        db_delete_specific_range(range_to_del)
        await query.answer(f"Deleted range {range_to_del}", show_alert=True)
        await query.edit_message_text(f"✅ **Range `{range_to_del}` deleted successfully.**", parse_mode="Markdown")

    elif data == "admin_broadcast":
        await query.answer()
        context.user_data["waiting_for_broadcast"] = True
        context.user_data["waiting_for_range"] = False
        await query.message.reply_text(
            "📢 **Send the message text you wish to broadcast to all bot users:**",
            parse_mode="Markdown"
        )

    elif data == "admin_clear":
        db_clear_managed_ranges()
        await query.answer("All ranges cleared from database!", show_alert=True)
        await query.edit_message_text("🗑 **All configured ranges have been cleared from database.**")


async def name_generate_callback_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    data = query.data
    chat_id = query.message.chat_id

    if data == "gen_menu":
        keyboard = InlineKeyboardMarkup([
            [InlineKeyboardButton("👨 Male Name", callback_data="gen_male"), InlineKeyboardButton("👩 Female Name", callback_data="gen_female")],
            [InlineKeyboardButton("🎲 Generate Both (Father & Name)", callback_data="gen_pair")]
        ])
        await query.edit_message_text("🏷️ **Name Generator Hub**\n\nSelect a category below. Names will be provided in copyable format:", parse_mode="Markdown", reply_markup=keyboard)
        return

    if not MALE_NAMES and not FEMALE_NAMES:
        await query.edit_message_text("❌ Name database is empty or file not found.")
        return

    if data == "gen_male":
        name = random.choice(MALE_NAMES) if MALE_NAMES else "N/A"
        text = f"👨 **Generated Male Name:**\n\n`{name}`"
    elif data == "gen_female":
        name = random.choice(FEMALE_NAMES) if FEMALE_NAMES else "N/A"
        text = f"👩 **Generated Female Name:**\n\n`{name}`"
    elif data == "gen_pair":
        m_name = random.choice(MALE_NAMES) if MALE_NAMES else "N/A"
        f_name = random.choice(FEMALE_NAMES) if FEMALE_NAMES else "N/A"
        text = f"🏷️ **Generated Names:**\n\n👨 Male / Father Name: `{m_name}`\n👩 Female Name: `{f_name}`"
    else:
        return

    keyboard = InlineKeyboardMarkup([
        [
            InlineKeyboardButton("🔄 Generate Another 🔤", callback_data=data),
        ],
        [
            InlineKeyboardButton("🔙 Back to Options 🔙", callback_data="gen_menu")
        ]
    ])

    try:
        await query.message.delete()
    except Exception as e:
        logging.warning(f"Could not delete previous name message: {e}")

    await context.bot.send_message(
        chat_id=chat_id,
        text=text,
        parse_mode="Markdown",
        reply_markup=keyboard
    )

async def name_menu_callback_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    keyboard = InlineKeyboardMarkup([
        [InlineKeyboardButton("👨 Male Name", callback_data="gen_male"), InlineKeyboardButton("👩 Female Name", callback_data="gen_female")],
        [InlineKeyboardButton("🎲 Generate Both (Father & Name)", callback_data="gen_pair")]
    ])
    await query.edit_message_text("🏷️ **Name Generator Hub**\n\nSelect a category below. Names will be provided in copyable format:", parse_mode="Markdown", reply_markup=keyboard)


async def user_provision_callback_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    data = query.data
    chat_id = query.message.chat_id
    managed_ranges = db_get_managed_ranges()

    if data == "srv_menu":
        await query.answer()
        services = sorted(list(set(r["service"] for r in managed_ranges)))
        keyboard = [
            [InlineKeyboardButton(f"🛡️ {srv} 🟢", callback_data=f"srv_{srv}")]
            for srv in services
        ]
        await query.edit_message_text(
            "🛠 **Select a Service:**",
            parse_mode="Markdown",
            reply_markup=InlineKeyboardMarkup(keyboard),
        )
        return

    if data.startswith("srv_"):
        selected_service = data.replace("srv_", "")
        matching_ranges = [r for r in managed_ranges if r["service"] == selected_service]

        keyboard = [
            [
                InlineKeyboardButton(
                    f"{r['flag']} {r['country']} ({r['range']}) 🔵",
                    callback_data=f"prov_{r['range']}",
                )
            ]
            for r in matching_ranges
        ]
        keyboard.append([InlineKeyboardButton("🔙 Back to Services 🔙", callback_data="srv_menu")])

        await query.edit_message_text(
            f"🛠 **Selected Service:** `{selected_service}`\n\n"
            f"👇 **Select Country / Range to allocate your number:**",
            parse_mode="Markdown",
            reply_markup=InlineKeyboardMarkup(keyboard),
        )

    elif data.startswith("prov_") or data.startswith("change_"):
        await query.answer()
        
        is_change_request = data.startswith("change_")
        selected_range = data.replace("prov_", "").replace("change_", "")
        matched_item = next((r for r in managed_ranges if r["range"] == selected_range), {})
        
        c_name, flag_icon = auto_detect_country_and_flag(
            matched_item.get("country", ""), selected_range
        )

        if is_change_request:
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

        res = await zebra.get_number(selected_range)
        meta = res.get("meta", {})

        if meta.get("code") == 0:
            row = res["data"]["rows"][0]
            allocated_num = row.get("number")
            
            api_country = row.get("country")
            target_country = api_country if api_country else matched_item.get("country", c_name)
            final_country_name, final_flag = auto_detect_country_and_flag(target_country, allocated_num)

            active_allocations[allocated_num] = {
                "chat_id": chat_id,
                "country": final_country_name,
                "flag": final_flag,
            }

            msg = (
                f"✅ **Number Allocated Successfully!**\n\n"
                f"📱 **Number:** `{allocated_num}`\n"
                f"{final_flag} **Country:** {final_country_name}"
            )

            keyboard = InlineKeyboardMarkup(
                [
                    [InlineKeyboardButton("🔄 Change Number 🟠", callback_data=f"change_{selected_range}")],
                    [InlineKeyboardButton("📢 Open OTP Channel 🔵", url=CHANNEL_URL)],
                    [InlineKeyboardButton("🔙 Back to Services 🔙", callback_data="srv_menu")]
                ]
            )

            if is_change_request:
                await loading_msg.edit_text(
                    msg, parse_mode="Markdown", reply_markup=keyboard
                )
            else:
                await query.edit_message_text(
                    msg, parse_mode="Markdown", reply_markup=keyboard
                )
        else:
            err_msg = res.get("message") or meta.get("error") or "Unknown error"
            msg = f"❌ **Failed to allocate number:**\nStatus Code: {meta.get('code')}\nDetails: {err_msg}"
            
            back_keyboard = InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Back to Services 🔙", callback_data="srv_menu")]])

            if is_change_request:
                await loading_msg.edit_text(msg, parse_mode="Markdown", reply_markup=back_keyboard)
            else:
                await query.edit_message_text(msg, parse_mode="Markdown", reply_markup=back_keyboard)


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
    text = update.message.text.strip()
    user_chat_id = update.effective_chat.id

    db_add_user(user_chat_id)

    if context.user_data.get("waiting_for_range"):
        if text.count("|") != 2:
            await update.message.reply_text(
                "⚠️ **Format Error!** Use: `Service | Country | Range`\n"
                "Example: `Facebook | Cambodia | 85531879XXX`",
                parse_mode="Markdown",
            )
            return

        parts = [p.strip() for p in text.split("|")]
        service, country, range_val = parts[0], parts[1], parts[2]
        c_name, detected_flag = auto_detect_country_and_flag(country, range_val)

        db_add_managed_range(service, c_name, detected_flag, range_val)
        
        context.user_data["waiting_for_range"] = False

        await update.message.reply_text(
            f"✅ **Successfully saved range to database!**\n"
            f"📌 **Service:** `{service}`\n"
            f"{detected_flag} **Country:** `{c_name}`\n"
            f"🔢 **Range:** `{range_val}`",
            parse_mode="Markdown",
            reply_markup=get_main_keyboard(),
        )
        return

    if context.user_data.get("waiting_for_broadcast"):
        context.user_data["waiting_for_broadcast"] = False
        all_users = db_get_all_users()
        
        if not all_users:
            all_users = [user_chat_id]

        status_msg = await update.message.reply_text(f"⏳ Sending broadcast message to {len(all_users)} users...")
        
        success, failed = 0, 0
        for uid in all_users:
            try:
                await context.bot.send_message(
                    chat_id=uid,
                    text=f"📢 **ANNOUNCEMENT** 📢\n\n{text}",
                    parse_mode="Markdown"
                )
                success += 1
            except Exception as e:
                failed += 1
                logging.error(f"Failed to broadcast to {uid}: {e}")

        await status_msg.edit_text(f"✅ **Broadcast Completed!**\n\n Successful: `{success}`\n❌ Failed: `{failed}`", parse_mode="Markdown")
        return

    if "Get Number" in text:
        managed_ranges = db_get_managed_ranges()

        if not managed_ranges:
            await update.message.reply_text(
                "⚠️ No ranges configured yet. An admin must configure ranges via `/admin`.",
                reply_markup=get_main_keyboard(),
            )
            return

        services = sorted(list(set(r["service"] for r in managed_ranges)))
        keyboard = [
            [InlineKeyboardButton(f"🛡️ {srv} 🟢", callback_data=f"srv_{srv}")]
            for srv in services
        ]
        await update.message.reply_text(
            "🛠 **Select a Service:**",
            parse_mode="Markdown",
            reply_markup=InlineKeyboardMarkup(keyboard),
        )
        return

    if "Name Generate" in text:
        keyboard = InlineKeyboardMarkup([
            [InlineKeyboardButton("👨 Male Name", callback_data="gen_male"), InlineKeyboardButton("👩 Female Name", callback_data="gen_female")],
            [InlineKeyboardButton("🎲 Generate Both (Father & Name)", callback_data="gen_pair")]
        ])
        await update.message.reply_text(
            "🏷️ **Name Generator Hub**\n\nSelect a category below. Names will be provided in copyable format:",
            parse_mode="Markdown",
            reply_markup=keyboard,
        )
        return

    if "Active Engine" in text:
        count = len(active_allocations)
        await update.message.reply_text(
            f"⚡ **Active Engine Status**\n\n"
            f"• Currently Active Allocations: `{count}`\n"
            f"• Background Polling: `Running (24/7)`",
            parse_mode="Markdown",
            reply_markup=get_main_keyboard(),
        )
        return

    if "Live Feed" in text:
        res = await zebra.get_live_access()
        meta = res.get("meta", {})
        if meta.get("code") == 0:
            rows = res.get("data", {}).get("rows", [])[:5]
            if not rows:
                feed_text = "🌐 No recent messages found in live feed."
            else:
                feed_text = "🌐 **Recent Live SMS Feed:**\n\n"
                for r in rows:
                    num = mask_phone_number(r.get("number", ""))
                    sender = r.get("sender", "Unknown")
                    msg = r.get("message", "")
                    feed_text += f"📱 `{num}` | 👤 `{sender}`\n💬 `{msg}`\n\n"
        else:
            feed_text = "❌ Failed to fetch live feed from ZebraSMS API."

        await update.message.reply_text(feed_text, parse_mode="Markdown", reply_markup=get_main_keyboard())
        return

    if "Referrals" in text:
        bot_user = (await context.bot.get_me()).username
        ref_link = f"https://t.me/{bot_user}?start=ref_{user_chat_id}"
        await update.message.reply_text(
            f"🎁 **Referral Program**\n\n"
            f"Invite your friends and earn rewards!\n\n"
            f"🔗 Your Invite Link:\n`{ref_link}`",
            parse_mode="Markdown",
            reply_markup=get_main_keyboard(),
        )
        return

    if "My Profile" in text:
        await update.message.reply_text(
            f"👤 **User Profile**\n\n"
            f"• Telegram ID: `{user_chat_id}`\n"
            f"• Status: `Active / Verified`",
            parse_mode="Markdown",
            reply_markup=get_main_keyboard(),
        )
        return

    if "Support Hub" in text:
        await update.message.reply_text(
            f"🎧 **Support Hub**\n\n"
            f"Need assistance or have issues? Contact our support team directly: {SUPPORT_USERNAME}",
            parse_mode="Markdown",
            reply_markup=get_main_keyboard(),
        )
        return


def main():
    if not TELEGRAM_BOT_TOKEN:
        logging.error("TELEGRAM_BOT_TOKEN environment variable is not set!")
        return

    # Start keep_alive server for uptime hosting (Render/Replit)
    keep_alive()

    app = ApplicationBuilder().token(TELEGRAM_BOT_TOKEN).build()

    # Command Handlers
    app.add_handler(CommandHandler("start", start_command))
    app.add_handler(CommandHandler("admin", cmd_admin))
