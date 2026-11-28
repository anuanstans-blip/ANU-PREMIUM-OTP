import asyncio
import logging
import os
import re
import random
import json
import time
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
from telegram.error import Forbidden, RetryAfter
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

# Safely parse ADMIN_ID, removing any accidental quotes or spaces
_admin_raw = os.getenv("ADMIN_ID", "0").strip(" '\"")
ADMIN_ID = int(_admin_raw) if _admin_raw.isdigit() else 0

# Safely parse CHANNEL_CHAT_ID (handling negative chat IDs for channels/groups)
_channel_raw = os.getenv("CHANNEL_CHAT_ID", "0").strip(" '\"")
CHANNEL_CHAT_ID = int(_channel_raw) if _channel_raw.lstrip("-").isdigit() else 0

CHANNEL_URL = os.getenv("CHANNEL_URL", "https://t.me/").strip(" '\"")

ZEBRASMS_API_KEY = os.getenv("ZEBRASMS_API_KEY", "Q32FDUSCCGF").strip(" '\"")
ZEBRASMS_BASE_URL = os.getenv("ZEBRASMS_BASE_URL", "https://api.zebrasms.com/api/v1").strip(" '\"")

MKNETWORK_API_KEY = os.getenv("MKNETWORK_API_KEY", "").strip(" '\"")
MKNETWORK_BASE_URL = os.getenv("MKNETWORK_BASE_URL", "https://mknetworkbd.com/api/v3/public").strip(" '\"")

VOLTXSMS_API_KEY = os.getenv("VOLTXSMS_API_KEY", "").strip(" '\"")
VOLTXSMS_BASE_URL = os.getenv(
    "VOLTXSMS_BASE_URL",
    "https://api.2oo9.cloud/MXS47FLFXBU/tnevs/@public/api",
).strip(" '\"")

SUPPORT_USERNAME = os.getenv("SUPPORT_USERNAME", "@anstans").strip(" '\"")

SUPABASE_URL = os.getenv("SUPABASE_URL", "").strip(" '\"")
SUPABASE_KEY = os.getenv("SUPABASE_KEY", "").strip(" '\"")

# Optional comma-separated group chat IDs for 30-minute DB range country broadcasts
_groups_raw = os.getenv("BROADCAST_GROUP_IDS", "").strip(" '\"")
known_group_ids: set[int] = {
    int(gid.strip())
    for gid in _groups_raw.split(",")
    if gid.strip().lstrip("-").isdigit()
}

# Initialize Supabase Client
supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY) if SUPABASE_URL and SUPABASE_KEY else None

logging.basicConfig(
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    level=logging.INFO,
)

# --- Global State & Async Queue ---
active_allocations = {}
seen_messages = set()
seen_mk_sms_ids = set()
seen_voltx_otp_ids = set()
MAX_SEEN_SIZE = 5000

# 30-minute per-country broadcast state for DB-saved ranges
COUNTRY_BROADCAST_COOLDOWN_SECONDS = 1800.0
last_country_broadcast_at: dict[str, float] = {}
_country_broadcast_lock = asyncio.Lock()

# High-throughput asynchronous dispatch queue
otp_dispatch_queue = asyncio.Queue()

# Track active background tasks per allocation key to prevent duplicate workers
active_worker_tasks = {}

# Fake OTP Generator State & Settings
fake_otp_settings = {
    "enabled": False,
    "interval": 15,
}

# Auto-Ranging Cache & Background Refresh State
LIVEACCESS_CACHE_TTL_SECONDS = 45.0
LIVEACCESS_REFRESH_INTERVAL_SECONDS = 30.0
_liveaccess_cache = {
    "ranges": [],
    "zebra_live_rows": [],
    "mk_access_items": [],
    "voltx_live_services": [],
    "updated_at": 0.0,
}
_liveaccess_lock = asyncio.Lock()

# Fallback default ranges for ZebraSMS
DEFAULT_RANGES_ZEBRA = [
    {"service": "WhatsApp", "country": "Ivory Coast", "flag": "🇨🇮", "range": "22501XXX", "provider": "zebrasms"},
]

# --- Social Media Sender ID Filter ---
BLOCKED_NON_SOCIAL_SENDERS = {
    "binance", "bybit", "kucoin", "okx", "coinbase", "kraken", "huobi", "htx",
    "mexc", "gate", "bitget", "crypto", "paypal", "wise", "revolut", "cashapp",
}

SOCIAL_MEDIA_KEYWORDS = (
    "facebook", "fb", "whatsapp", "wa", "instagram", "insta", "ig",
    "telegram", "tg", "tiktok", "snapchat", "snap", "twitter", "x.com",
    "linkedin", "viber", "line", "zalo", "wechat", "kakao", "messenger",
    "threads", "discord", "imo",
)


def is_allowed_social_sender(sender_name: str) -> bool:
    name = (sender_name or "").strip()
    if not name:
        return False
    lower = name.lower()
    if any(blocked in lower for blocked in BLOCKED_NON_SOCIAL_SENDERS):
        return False
    return any(kw in lower for kw in SOCIAL_MEDIA_KEYWORDS)


# --- Global Name Generator Helper (1,000 First Names & 1,000 Last Names) ---
FIRST_NAMES_1000 = [
    "Getu", "Ashenafi", "Noah", "Darge", "Ethan", "Bereket", "Mason", "Logan",
    "Emma", "Sophia", "Olivia", "Mewuded", "Mia", "Isabella", "Belayneh", "Kalkidan",
    "Mekdes", "Benjamin", "Elijah", "Girma", "Charlotte", "Amelia", "Tagay", "Abebe",
    "Kebede", "Tigist", "Selamawit", "Yohannes", "Dawit", "Tadesse", "Hanna", "Meron",
    "Biniyam", "Samuel", "Kidist", "Solomon", "Rahel", "Eyob", "Abel", "Saron",
    "Nahom", "Eyerusalem", "Mikiyas", "Tsion", "Natnael", "Bethlehem", "Henok", "Hiwot",
    "Yonatan", "Lidya", "Amanuel", "Bezawit", "Fikadu", "Alemitu", "Getachew", "Meseret",
    "Tesfaye", "Zewditu", "Worku", "Almaz", "Habtamu", "Aster", "Mulu", "Genet",
    "Tsegaye", "Tirunesh", "Ayalew", "Birtukan", "Wondimu", "Senait", "Demeke", "Frehiwot",
    "Kassa", "Etalemahu", "Gashaw", "Mebrat", "Shiferaw", "Tigest", "Addisu", "Yordanos",
    "Liam", "Oliver", "James", "Henry", "Lucas", "Alexander", "Sebastian", "Jack",
    "Owen", "Theodore", "Mateo", "Leo", "Aiden", "Samuel", "Wyatt", "John",
    "Julian", "Levi", "Isaac", "Gabriel", "Anthony", "Dylan", "Lincoln", "Jaxon",
    "Asher", "Christopher", "Josiah", "Andrew", "Thomas", "Joshua", "Ezra", "Hudson",
    "Charles", "Caleb", "Isaiah", "Ryan", "Nathan", "Adrian", "Christian", "Maverick",
    "Colton", "Elias", "Aaron", "Eli", "Landon", "Jonathan", "Nolan", "Hunter",
    "Cameron", "Connor", "Brooks", "Jordan", "Angel", "Roman", "Easton", "Wesley",
    "Ian", "Jason", "Axel", "Beckett", "Silas", "Sawyer", "Declan", "Waylon",
    "Weston", "Evan", "Emmett", "Harrison", "Micah", "Ryder", "Beau", "Damian",
    "Greyson", "Jasper", "Braxton", "Kaiden", "Zayden", "Kayden", "Parker", "Bentley",
    "Ava", "Evelyn", "Harper", "Camila", "Gianna", "Abigail", "Luna", "Ella",
    "Elizabeth", "Sofia", "Emily", "Avery", "Mila", "Scarlett", "Eleanor", "Madison",
    "Layla", "Penelope", "Aria", "Chloe", "Grace", "Ellie", "Nora", "Hazel",
    "Zoey", "Riley", "Victoria", "Lily", "Aurora", "Violet", "Nova", "Hannah",
    "Emilia", "Zoe", "Stella", "Everly", "Isla", "Leah", "Lillian", "Addison",
    "Willow", "Lucy", "Paisley", "Natalie", "Naomi", "Eliana", "Brooklyn", "Elena",
    "Aubrey", "Claire", "Ivy", "Kinsley", "Audrey", "Maya", "Genesis", "Skylar",
    "Bella", "Aaliyah", "Madelyn", "Savannah", "Anna", "Delilah", "Caroline", "Kennedy",
    "Valentina", "Ruby", "Sophie", "Alice", "Gabriella", "Sadie", "Ariana", "Allison",
    "Hailey", "Autumn", "Nevaeh", "Natalia", "Quinn", "Josephine", "Sarah", "Cora",
    "Emery", "Samantha", "Piper", "Leilani", "Eva", "Everleigh", "Madeline", "Lydia",
    "Jade", "Peyton", "Brielle", "Adeline", "Vivian", "Rylee", "Clara", "Raelynn",
    "Melanie", "Melody", "Julia", "Athena", "Maria", "Liliana", "Arya", "Elise",
    "Mohamed", "Ahmed", "Ali", "Omar", "Youssef", "Ibrahim", "Mahmoud", "Mustafa",
    "Hassan", "Hussein", "Tariq", "Zayd", "Bilal", "Hamza", "Karim", "Adel",
    "Amir", "Samir", "Rami", "Nabil", "Fadi", "Walid", "Rashid", "Sami",
    "Fatima", "Aisha", "Mariam", "Zainab", "Layla", "Noor", "Yasmin", "Huda",
    "Salma", "Nadia", "Amira", "Rania", "Dina", "Samira", "Farida", "Leila",
    "Amina", "Khadija", "Mona", "Hana", "Reem", "Sana", "Lina", "Jamila",
    "Chinedu", "Emeka", "Obinna", "Ikechukwu", "Kelechi", "Uche", "Nnamdi", "Chidi",
    "Tunde", "Segun", "Femi", "Ade", "Kunle", "Bamidele", "Yemi", "Rotimi",
    "Kwame", "Kofi", "Kwabena", "Yaw", "Kojo", "Akosua", "Ama", "Efua",
    "Adjoa", "Abena", "Chioma", "Ngozi", "Amaka", "Ifunanya", "Chidinma", "Zainab",
    "Sibusiso", "Sipho", "Thabo", "Bongani", "Lehlohonolo", "Kgosi", "Kagiso", "Katlego",
    "Lerato", "Naledi", "Refilwe", "Thandi", "Nomusa", "Zanele", "Ayanda", "Buhle",
    "Kamau", "Mwangi", "Njoroge", "Ochieng", "Otieno", "Wanjiku", "Njeri", "Wambui",
    "Akinyi", "Auma", "Juma", "Baraka", "Amani", "Zawadi", "Neema", "Furaha",
    "Jean", "Pierre", "Luc", "Marc", "Paul", "Henri", "Louis", "Antoine",
    "Claude", "Michel", "Philippe", "Julien", "Nicolas", "Thomas", "Sebastien", "Laurent",
    "Marie", "Claire", "Sophie", "Camille", "Julie", "Celeste", "Amelie", "Margaux",
    "Manon", "Chloe", "Lea", "Ines", "Lou", "Adele", "Jeanne", "Marguerite",
    "Lukas", "Leon", "Tim", "Finn", "Jonas", "Maximilian", "Felix", "Moritz",
    "Niklas", "Jan", "Tobias", "Stefan", "Matthias", "Andreas", "Christian", "Daniel",
    "Anna", "Lena", "Lea", "Marie", "Hannah", "Sophie", "Emilia", "Mia",
    "Clara", "Johanna", "Lina", "Amelie", "Katharina", "Sarah", "Julia", "Laura",
    "Marco", "Luca", "Matteo", "Lorenzo", "Alessandro", "Andrea", "Davide", "Simone",
    "Federico", "Riccardo", "Filippo", "Tommaso", "Gabriele", "Mattia", "Edoardo", "Pietro",
    "Giulia", "Sofia", "Aurora", "Alice", "Ginevra", "Emma", "Giorgia", "Greta",
    "Martina", "Chiara", "Beatrice", "Anna", "Sara", "Francesca", "Vittoria", "Elena",
    "Alejandro", "Diego", "Carlos", "Javier", "Miguel", "Jose", "Pablo", "Daniel",
    "David", "Adrian", "Alvaro", "Sergio", "Jorge", "Raul", "Ruben", "Ivan",
    "Lucia", "Sofia", "Martina", "Maria", "Julia", "Paula", "Valeria", "Emma",
    "Daniela", "Carla", "Alba", "Noa", "Carmen", "Claudia", "Irene", "Elena",
    "Lucas", "Gabriel", "Matheus", "Pedro", "Rafael", "Gustavo", "Felipe", "Bruno",
    "Eduardo", "Guilherme", "Henrique", "Murilo", "Vitor", "Leonardo", "Caio", "Thiago",
    "Helena", "Alice", "Laura", "Manuela", "Sophia", "Isabella", "Luisa", "Heloisa",
    "Valentina", "Julia", "Livia", "Lorena", "Giovanna", "Mariana", "Beatriz", "Ana",
    "Aarav", "Vivaan", "Aditya", "Vihaan", "Arjun", "Sai", "Reyansh", "Ayaan",
    "Krishna", "Ishaan", "Rohan", "Kabir", "Ananya", "Aadhya", "Diya", "Saanvi",
    "Anika", "Myra", "Aarohi", "Ira", "Pari", "Navya", "Kiara", "Prisha",
    "Haruto", "Riku", "Sora", "Kaito", "Yuto", "Ren", "Haruki", "Souta",
    "Hinata", "Yuma", "Takumi", "Daiki", "Kenta", "Naoki", "Kenji", "Shinji",
    "Yuna", "Aoi", "Himari", "Mei", "Hina", "Sakura", "Ichika", "Akari",
    "Rin", "Mio", "Koharu", "Yui", "Saki", "Nanami", "Haruka", "Ayaka",
    "Min-jun", "Seo-jun", "Do-yun", "Ye-jun", "Ju-won", "Ji-ho", "Joon-woo", "Hyun-woo",
    "Ji-woo", "Seo-yeon", "Ha-eun", "Ji-yoon", "Min-seo", "Su-bin", "Ye-eun", "Chae-won",
    "Wei", "Fang", "Lei", "Jing", "Min", "Qiang", "Li", "Yan",
    "Jun", "Yong", "Hui", "Ming", "Xin", "Bo", "Ping", "Bin",
    "Aleksandr", "Dmitry", "Maksim", "Ivan", "Artem", "Nikita", "Mikhail", "Daniil",
    "Egor", "Matvey", "Andrey", "Kirill", "Ilya", "Roman", "Sergey", "Anton",
    "Sofia", "Anna", "Maria", "Alisa", "Viktoria", "Polina", "Daria", "Ksenia",
    "Kira", "Varvara", "Eva", "Milana", "Veronika", "Ekaterina", "Arina", "Liam",
    "Oisin", "Fionn", "Cillian", "Tadhg", "Cormac", "Conor", "Darragh", "Caoimhe",
    "Saoirse", "Fiadh", "Aoife", "Roisin", "Niamh", "Ciara", "Maeve", "Jack",
    "Charlie", "George", "Thomas", "William", "Leo", "Oscar", "Harry", "Charlotte",
    "Amelia", "Olivia", "Mia", "Isla", "Ava", "Grace", "Chloe", "Soren",
    "Emil", "Kasper", "Mikkel", "Frederik", "Magnus", "Lukas", "Oliver", "Freja",
    "Alma", "Ida", "Clara", "Emma", "Ella", "Sofia", "Astrid", "Kacper",
    "Antoni", "Jan", "Aleksander", "Franciszek", "Jakub", "Mikolaj", "Leon", "Zuzanna",
    "Zofia", "Hanna", "Julia", "Maja", "Alicja", "Oliwia", "Laura", "Nikos",
    "Giorgos", "Kostas", "Dimitris", "Giannis", "Panagiotis", "Christos", "Vasilis", "Elena",
    "Maria", "Katerina", "Eleni", "Sofia", "Georgia", "Christina", "Ioanna", "Mehmet",
    "Mustafa", "Ahmet", "Emre", "Burak", "Can", "Kaan", "Kerem", "Zeynep",
    "Elif", "Defne", "Azra", "Eylül", "Nehir", "Mira", "Ada", "Farhan",
    "Rizky", "Dimas", "Bagas", "Bayu", "Arif", "Fajar", "Wahyu", "Putri",
    "Siti", "Ayu", "Dewi", "Intan", "Lestari", "Ratna", "Sri", "Somchai",
    "Somsak", "Somporn", "Anan", "Chai", "Prasert", "Kittipong", "Nattapong", "Malee",
    "Pornthip", "Supaporn", "Wanida", "Siriporn", "Kannika", "Nok", "Ploy", "Minh",
    "Hieu", "Nam", "Khoa", "Duc", "Tuan", "Hung", "Dung", "Linh",
    "Huong", "Thao", "Ngoc", "Trang", "Mai", "Lan", "Hanh", "Mateo",
    "Santiago", "Sebastian", "Matias", "Nicolas", "Samuel", "Alejandro", "Diego", "Valeria",
    "Sofía", "Isabella", "Martina", "Lucía", "Camila", "Mariana", "Salomé", "Jaden",
    "Tyler", "Brandon", "Justin", "Kevin", "Austin", "Caleb", "Zachary", "Kayla",
    "Brianna", "Jasmine", "Destiny", "Sierra", "Alexis", "Kendra", "Morgan", "Rory",
    "Callum", "Finlay", "Lachlan", "Hamish", "Fraser", "Angus", "Blair", "Isla",
    "Freya", "Ailsa", "Morag", "Kirsty", "Fiona", "Skye", "Maisie", "Rhys",
    "Owain", "Gareth", "Dylan", "Evan", "Morgan", "Toby", "Ellis", "Seren",
    "Carys", "Megan", "Bethan", "Lowri", "Nia", "Ffion", "Rhian", "Marek",
    "Jakub", "Tomas", "Martin", "Lukas", "David", "Filip", "Ondrej", "Tereza",
    "Eliska", "Anna", "Adela", "Karolina", "Kristyna", "Natálie", "Viktorie", "Bence",
    "Levente", "Mate", "Dominik", "Marcell", "Balazs", "Dániel", "Zsombor", "Hanna",
    "Anna", "Lili", "Zoé", "Emma", "Luca", "Nora", "Mira", "Onni",
    "Elias", "Leo", "Oliver", "Väinö", "Toivo", "Aatos", "Eino", "Aino",
    "Aada", "Eevi", "Lilja", "Helmi", "Venla", "Sofia", "Olivia", "Artyom",
    "Bohdan", "Maksym", "Danylo", "Vladyslav", "Oleksandr", "Andriy", "Denys", "Sofiia",
    "Anastasiia", "Viktoriia", "Solomiia", "Veronika", "Zlata", "Polina", "Milana", "Luka",
    "Marko", "Filip", "Jakov", "Roko", "Fran", "Ivan", "Petar", "Lucija",
    "Mia", "Ema", "Nika", "Marta", "Rita", "Sara", "Klara", "Lukas",
    "Nojus", "Matas", "Jokubas", "Dominykas", "Emilis", "Adomas", "Kajus", "Liepa",
    "Amelija", "Emilija", "Gabija", "Kamile", "Austeja", "Viktorija", "Kotrina", "Roberts",
    "Markuss", "Gustavs", "Emils", "Arturs", "Kaspars", "Janis", "Toms", "Sofija",
    "Emilija", "Amelija", "Marta", "Anna", "Alise", "Paula", "Kate", "Rasmus",
    "Oliver", "Hugo", "Liam", "Matteo", "Lucas", "Leon", "Elliot", "Elsa",
    "Alice", "Maja", "Astrid", "Alma", "Freja", "Ella", "Vera", "Sander",
    "Rasmus", "Oliver", "Robin", "Marten", "Henri", "Artur", "Kristjan", "Mia",
    "Lenna", "Marta", "Sandra", "Sofia", "Eliise", "Emma", "Lisandra", "Luka",
    "Nik", "Jan", "Mark", "Filip", "Jakob", "Ziga", "Tilen", "Ema",
    "Sara", "Mia", "Hana", "Zala", "Lara", "Julija", "Kaja", "Stefan",
    "Nikola", "Luka", "Marko", "Uros", "Filip", "Milos", "Aleksa", "Dunja",
    "Sara", "Teodora", "Tara", "Milica", "Sofija", "Lena", "Una", "Kaloyan",
    "Aleksandar", "Georgi", "Martin", "Dimitar", "Ivan", "Nikolay", "Viktor", "Viktoria",
    "Maria", "Nikol", "Raya", "Yoana", "Sofia", "Gabriela", "Daria", "Andrei",
    "David", "Matei", "Gabriel", "Alexandru", "Stefan", "Luca", "Filip", "Maria",
    "Elena", "Sofia", "Ioana", "Eva", "Antonia", "Amalia", "Ana", "Sami",
    "Karim", "Youssef", "Bilal", "Rami", "Ziad", "Anas", "Omar", "Yasmine",
    "Salma", "Nour", "Mariam", "Ines", "Aya", "Malak", "Farah", "Ayoub",
    "Hamza", "Mehdi", "Yassine", "Othman", "Amine", "Adil", "Saad", "Salma",
    "Yasmine", "Hiba", "Aya", "Kenza", "Meryem", "Rania", "Zineb", "Tarek",
    "Fares", "Ziad", "Karim", "Marwan", "Sherif", "Hany", "Tamer", "Habiba",
    "Jana", "Malak", "Salma", "Farida", "Mariam", "Nada", "Laila", "Abdullah",
    "Fahad", "Khalid", "Saud", "Faisal", "Sultan", "Tariq", "Nawaf", "Noura",
    "Sara", "Reem", "Lama", "Alia", "Hessa", "Maha", "Joud", "Rashid",
    "Hamdan", "Majid", "Saeed", "Khalifa", "Sultan", "Zayed", "Mansoor", "Maryam",
    "Fatima", "Mouza", "Shamma", "Alya", "Maitha", "Latifa", "Hind", "Zain",
    "Hassan", "Ali", "Hadi", "Abbas", "Mustafa", "Jaafar", "Mahdi", "Zahra",
    "Fatima", "Zainab", "Hawraa", "Maryam", "Nur", "Khadija", "Amina", "Darius",
    "Cedric", "Malik", "Jalen", "Terrence", "Desmond", "Andre", "Kendall", "Imani",
    "Simone", "Tiana", "Nia", "Sasha", "Zuri", "Amara", "Kaya", "Matteo",
    "Lorenzo", "Diego", "Gael", "Thiago", "Bruno", "Liam", "Camila", "Valeria",
    "Sofía", "Emma", "Renata", "Regina", "Ximena", "Paula", "Joaquin", "Ignacio",
    "Tomas", "Benjamin", "Agustin", "Vicente", "Maximiliano", "Martin", "Emilia", "Isidora",
    "Maite", "Trinidad", "Amanda", "Josefa", "Florencia", "Antonia", "Juan", "Carlos",
    "Luis", "Jorge", "Pedro", "Miguel", "Andres", "Rafael", "Valentina", "Mariana",
    "Valeria", "Gabriela", "Sara", "Salome", "Isabella", "Luciana", "Franco", "Bautista",
    "Thiago", "Benicio", "Santino", "Joaquin", "Mateo", "Valentino", "Olivia", "Martina",
    "Isabella", "Catalina", "Emilia", "Sofia", "Delfina", "Alma", "Ezekiel", "Caleb",
    "Malachi", "Jeremiah", "Gideon", "Micah", "Jonah", "Ezra", "Miriam", "Ruth",
    "Esther", "Naomi", "Deborah", "Rachel", "Rebecca", "Leah", "Kieran", "Callum",
    "Declan", "Rowan", "Gavin", "Colin", "Graham", "Stuart", "Fiona", "Heather",
    "Catriona", "Eilidh", "Mhairi", "Rhona", "Ailsa", "Shona", "Rohan", "Vikram",
    "Siddharth", "Pranav", "Nikhil", "Varun", "Karan", "Akshay", "Pooja", "Divya",
    "Sneha", "Neha", "Priya", "Kavita", "Anjali", "Shruti", "Bilal", "Usman",
    "Hamza", "Zain", "Tariq", "Saad", "Haris", "Umer", "Ayesha", "Zainab",
    "Fatima", "Hira", "Sana", "Mahnoor", "Laiba", "Areeba", "Rafiq", "Tanvir",
    "Shakil", "Mahmud", "Asif", "Kamal", "Farid", "Noman", "Nusrat", "Sharmin",
    "Tasnim", "Farhana", "Mim", "Raisa", "Sabrina", "Sumaiya", "Jian", "Hao",
    "Zhi", "Chen", "Yu", "Kai", "Feng", "Long", "Mei", "Xue",
    "Ling", "Hong", "Qing", "Shu", "Lan", "Ying", "Aung", "Kyaw",
    "Zaw", "Min", "Htet", "Naing", "Tun", "Win", "Su", "Hnin",
    "Thandar", "Aye", "Khin", "Ei", "May", "Sandar", "Sokha", "Dara",
    "Rithy", "Vanna", "Sophea", "Chan", "Visal", "Bora", "Bopha", "Sreymom",
    "Channary", "Kunthea", "Sotheary", "Leakena", "Chenda", "Pich", "Kham", "Souk",
    "Boun", "Phone", "Seng", "Vilay", "Thong", "Xay", "Chanthaly", "Vilayphone",
    "Bounpheng", "Souksavanh", "Khamla", "Malay", "Noy", "Latsamy", "Tenzing", "Karma",
    "Dorje", "Sonam", "Tashi", "Pemba", "Sherpa", "Phurba", "Yangchen", "Pema",
    "Dolma", "Tsering", "Dechen", "Choden", "Kunsang", "Lhamo", "Bikash", "Prakash",
    "Sujan", "Ramesh", "Dipendra", "Sandeep", "Binod", "Kiran", "Sita", "Gita",
    "Rita", "Sabita", "Kabita", "Sunita", "Anita", "Pabitra", "Nuwan", "Kasun",
    "Lahiru", "Dinesh", "Chamara", "Dilshan", "Tharindu", "Mahesh", "Nimali", "Dilani",
    "Chamari", "Tharushi", "Kavindi", "Sanduni", "Hashini", "Ruwanthi", "Moussa", "Mamady",
    "Sekou", "Oumar", "Ibrahima", "Boubacar", "Amadou", "Modibo", "Fatoumata", "Mariam",
    "Kadiatou", "Aissatou", "Hawa", "Aminata", "Bintou", "Salimata", "Lamine", "Mamadou",
    "Cheikh", "Ousmane", "Demba", "Moussa", "Abdou", "Babacar", "Ndeye", "Astou",
    "Khady", "Awa", "Sokhna", "Ramatoulaye", "Coumba", "Koffi", "Kouame", "Yao",
    "Konan", "Aka", "Brou", "Kouadio", "N'Guessan", "Aya", "Affoue", "Amenan",
    "Lou", "Adjoua", "Akissi", "Marie", "Christelle", "Fofana", "Konate", "Coulibaly",
    "Traore", "Diaby", "Sangare", "Toure", "Ouattara", "Maimouna", "Rokia", "Djene",
    "Korotoumou", "Djeneba", "Fanta", "Sali", "Kadia", "Tendai", "Tatenda", "Takudzwa",
    "Tinashe", "Kudzai", "Farai", "Munyaradzi", "Simbarashe", "Rumbidzai", "Chiedza", "Nyasha",
    "Tariro", "Rutendo", "Tapiwa", "Ruvimbo", "Chenai", "Thabo", "Kabelo", "Tumelo",
    "Mpho", "Katlego", "Kagiso", "Neo", "Tshepo", "Boitumelo", "Lerato", "Keabetswe",
    "Gorata", "Onalenna", "Lesego", "Gaone", "Masego", "Chanda", "Bwalya", "Mutale",
    "Mwamba", "Mulenga", "Tembo", "Phiri", "Banda", "Mutinta", "Natasha", "Chileshe",
    "Mumba", "Bupe", "Mapalo", "Kabwe", "Faith", "Amos", "Patrick", "Joseph",
    "Moses", "Peter", "John", "Stephen", "David", "Grace", "Sarah", "Esther",
    "Mercy", "Joy", "Peace", "Hope", "Gloria", "Kofi", "Kwame", "Yaw",
    "Kojo", "Kwesi", "Kwadwo", "Kobby", "Ebo", "Esi", "Afia", "Akua",
    "Yaa", "Adwoa", "Mansah", "Serwaa", "Pokuaa",
]

LAST_NAMES_1000 = [
    "Geremew", "Abriham", "Williams", "Kassa", "Jones", "Kondara", "Dawit",
    "Wilson", "Taylor", "Anderson", "Mulugeta", "Dachasa", "White", "Harris",
    "Martin", "Takele", "Garcia", "Martinez", "Munuro", "Asaminew", "Rodriguez", "Tadesse",
    "Kebede", "Alemu", "Bekele", "Hailu", "Tesfaye", "Girma", "Wolde", "Mekonnen",
    "Desta", "Negash", "Gebre", "Haile", "Abera", "Assefa", "Worku", "Lemma",
    "Getachew", "Tsegaye", "Ayalew", "Beshah", "Demissie", "Fekadu", "Mengistu", "Tefera",
    "Wondimu", "Yilma", "Zewde", "Belay", "Chala", "Diriba", "Feyisa", "Gemechu",
    "Gobena", "Hordofa", "Jaleta", "Ketema", "Leta", "Merga", "Olana", "Roba",
    "Tufa", "Wakjira", "Abate", "Admassu", "Amare", "Anteneh", "Araya", "Ayenew",
    "Bantie", "Bayu", "Bireda", "Bogale", "Dagne", "Damtew", "Dejene", "Derese",
    "Smith", "Johnson", "Brown", "Davis", "Miller", "Moore", "Jackson", "Thompson",
    "Lee", "Perez", "Lewis", "Robinson", "Walker", "Young", "Allen", "King",
    "Wright", "Scott", "Torres", "Nguyen", "Hill", "Flores", "Green", "Adams",
    "Nelson", "Baker", "Hall", "Rivera", "Campbell", "Mitchell", "Carter", "Roberts",
    "Gomez", "Phillips", "Evans", "Turner", "Diaz", "Parker", "Cruz", "Edwards",
    "Collins", "Reyes", "Stewart", "Morris", "Morales", "Murphy", "Cook", "Rogers",
    "Gutierrez", "Ortiz", "Morgan", "Cooper", "Peterson", "Bailey", "Reed", "Kelly",
    "Howard", "Ramos", "Kim", "Cox", "Ward", "Richardson", "Watson", "Brooks",
    "Chavez", "Wood", "James", "Bennett", "Gray", "Mendoza", "Ruiz", "Hughes",
    "Price", "Alvarez", "Castillo", "Sanders", "Patel", "Myers", "Long", "Ross",
    "Foster", "Jimenez", "Powell", "Jenkins", "Perry", "Russell", "Sullivan", "Bell",
    "Coleman", "Butler", "Henderson", "Barnes", "Gonzales", "Fisher", "Vasquez", "Simmons",
    "Romero", "Jordan", "Patterson", "Alexander", "Hamilton", "Graham", "Reynolds", "Griffin",
    "Wallace", "Moreno", "West", "Cole", "Hayes", "Bryant", "Herrera", "Gibson",
    "Ellis", "Tran", "Medina", "Aguilar", "Stevens", "Murray", "Ford", "Castro",
    "Marshall", "Owens", "Harrison", "Fernandez", "Mcdonald", "Woods", "Washington", "Kennedy",
    "Wells", "Vargas", "Henry", "Chen", "Freeman", "Webb", "Tucker", "Guzman",
    "Burns", "Crawford", "Olson", "Simpson", "Porter", "Hunter", "Gordon", "Mendez",
    "Silva", "Shaw", "Snyder", "Mason", "Dixon", "Munoz", "Hunt", "Hicks",
    "Holmes", "Palmer", "Wagner", "Black", "Robertson", "Boyd", "Rose", "Stone",
    "Salazar", "Fox", "Warren", "Mills", "Meyer", "Rice", "Schmidt", "Garza",
    "Daniels", "Ferguson", "Nichols", "Stephens", "Soto", "Weaver", "Ryan", "Gardner",
    "Payne", "Grant", "Dunn", "Kelley", "Spencer", "Hawkins", "Arnold", "Pierce",
    "Vazquez", "Hansen", "Peters", "Santos", "Hart", "Bradley", "Knight", "Elliott",
    "Cunningham", "Duncan", "Armstrong", "Hudson", "Carroll", "Lane", "Riley", "Andrews",
    "Johnston", "Ray", "Carr", "Cole", "Richards", "Willis", "Matthews", "Chapman",
    "Lawrence", "Garrett", "Vargas", "Watkins", "Wheeler", "Larson", "Carlson", "Harper",
    "George", "Greene", "Burke", "Guzman", "Morrison", "Munoz", "Jacobs", "O'brien",
    "Lawson", "Franklin", "Lynch", "Bishop", "Carr", "Salazar", "Gates", "Moreno",
    "Alvarado", "Barton", "Bates", "Becker", "Beck", "Beasley", "Bauer", "Barker",
    "Mohamed", "Ahmed", "Ali", "Omar", "Hassan", "Hussein", "Ibrahim", "Mahmoud",
    "Mustafa", "Youssef", "Saleh", "Abdullah", "Mansour", "Khoury", "Haddad", "Farah",
    "Darwish", "Sabbagh", "Nassar", "Bazzi", "Harb", "Saad", "Zahr", "El-Masri",
    "Al-Mansoor", "Al-Hashimi", "Al-Qahtani", "Al-Ghamdi", "Al-Harbi", "Al-Otaibi", "Al-Dosari", "Al-Shammari",
    "Okafor", "Nwosu", "Eze", "Okonkwo", "Obi", "Nwachukwu", "Adeyemi", "Adebayo",
    "Ogunleye", "Balogun", "Akintola", "Bakare", "Mensah", "Boateng", "Owusu", "Appiah",
    "Kumi", "Darko", "Amoah", "Agyeman", "Tetteh", "Quaye", "Addo", "Asante",
    "Ndlovu", "Khumalo", "Mokoena", "Dlamini", "Zulu", "Sithole", "Mahlangu", "Nkosi",
    "Van Der Merwe", "Botha", "Pretorius", "Venter", "Nel", "Fourie", "Jacobs", "Coetzee",
    "Kamau", "Mwangi", "Njoroge", "Ochieng", "Otieno", "Wanjiku", "Kimani", "Karanja",
    "Mutua", "Kiplagat", "Rotich", "Korir", "Chepkoech", "Jepchirchir", "Kiptoo", "Sang",
    "Martin", "Bernard", "Dubois", "Thomas", "Robert", "Richard", "Petit", "Durand",
    "Leroy", "Moreau", "Simon", "Laurent", "Lefebvre", "Michel", "Garcia", "David",
    "Bertrand", "Roux", "Vincent", "Fournier", "Morel", "Girard", "Andre", "Mercier",
    "Dupont", "Lambert", "Bonnet", "Francois", "Martinez", "Legrand", "Garnier", "Faure",
    "Muller", "Schmidt", "Schneider", "Fischer", "Weber", "Meyer", "Wagner", "Becker",
    "Schulz", "Hoffmann", "Schafer", "Koch", "Bauer", "Richter", "Klein", "Wolf",
    "Schroder", "Neumann", "Schwarz", "Zimmermann", "Braun", "Hofmann", "Hartmann", "Lange",
    "Schmitt", "Werner", "Schmitz", "Krause", "Meier", "Lehmann", "Schmid", "Schulze",
    "Rossi", "Russo", "Ferrari", "Esposito", "Bianchi", "Romano", "Colombo", "Ricci",
    "Marino", "Greco", "Bruno", "Gallo", "Conti", "De Luca", "Mancini", "Costa",
    "Giordano", "Rizzo", "Lombardi", "Moretti", "Barbieri", "Fontana", "Santoro", "Mariani",
    "Rinaldi", "Caruso", "Ferrara", "Galli", "Martini", "Leone", "Longo", "Gentile",
    "Gonzalez", "Rodriguez", "Fernandez", "Lopez", "Martinez", "Sanchez", "Perez", "Gomez",
    "Martin", "Jimenez", "Ruiz", "Hernandez", "Diaz", "Moreno", "Alvarez", "Munoz",
    "Romero", "Alonso", "Gutierrez", "Navarro", "Torres", "Dominguez", "Vazquez", "Ramos",
    "Gil", "Ramirez", "Serrano", "Blanco", "Molina", "Morales", "Suarez", "Ortega",
    "Silva", "Santos", "Oliveira", "Souza", "Rodrigues", "Ferreira", "Alves", "Pereira",
    "Lima", "Gomes", "Costa", "Ribeiro", "Martins", "Carvalho", "Almeida", "Lopes",
    "Soares", "Fernandes", "Vieira", "Barbosa", "Rocha", "Dias", "Nascimento", "Castro",
    "Sharma", "Verma", "Gupta", "Malhotra", "Mehta", "Joshi", "Nair", "Reddy",
    "Rao", "Iyer", "Menon", "Kumar", "Singh", "Kaur", "Patil", "Deshmukh",
    "Chatterjee", "Banerjee", "Mukherjee", "Bose", "Sen", "Das", "Roy", "Ghosh",
    "Sato", "Suzuki", "Takahashi", "Tanaka", "Watanabe", "Ito", "Yamamoto", "Nakamura",
    "Kobayashi", "Kato", "Yoshida", "Yamada", "Sasaki", "Yamaguchi", "Matsumoto", "Inoue",
    "Kimura", "Hayashi", "Shimizu", "Yamazaki", "Mori", "Abe", "Ikeda", "Hashimoto",
    "Kim", "Lee", "Park", "Choi", "Jung", "Kang", "Cho", "Yoon",
    "Jang", "Lim", "Han", "Shin", "Seo", "Kwon", "Hwang", "Ahn",
    "Wang", "Li", "Zhang", "Liu", "Chen", "Yang", "Huang", "Zhao",
    "Wu", "Zhou", "Xu", "Sun", "Ma", "Zhu", "Hu", "Guo",
    "Lin", "He", "Gao", "Luo", "Zheng", "Liang", "Xie", "Song",
    "Ivanov", "Smirnov", "Kuznetsov", "Popov", "Sokolov", "Lebedev", "Kozlov", "Novikov",
    "Morozov", "Petrov", "Volkov", "Solovyov", "Vasilyev", "Zaytsev", "Pavlov", "Semyonov",
    "Golubev", "Vinogradov", "Bogdanov", "Vorobyov", "Fyodorov", "Mikhaylov", "Belyayev", "Tarasov",
    "Murphy", "Kelly", "Byrne", "Ryan", "O'Connor", "O'Neill", "Walsh", "Reilly",
    "Doyle", "McCarthy", "Gallagher", "Doherty", "Kennedy", "Lynch", "Murray", "Quinn",
    "Nowak", "Kowalski", "Wisniewski", "Wojcik", "Kowalczyk", "Kaminski", "Lewandowski", "Zielinski",
    "Szymanski", "Wozniak", "Dabrowski", "Kozlowski", "Jankowski", "Mazur", "Wojciechowski", "Krawczyk",
    "Papadopoulos", "Vlachos", "Angelopoulos", "Nikolaou", "Georgiou", "Petrou", "Antoniou", "Christodoulou",
    "Yilmaz", "Kaya", "Demir", "Sahin", "Celik", "Yildiz", "Yildirim", "Ozturk",
    "Aydin", "Ozdemir", "Arslan", "Dogan", "Kilic", "Aslan", "Cetin", "Kara",
    "Pratama", "Saputra", "Hidayat", "Nugroho", "Wijaya", "Santoso", "Kurniawan", "Setiawan",
    "Lestari", "Wulandari", "Utami", "Rahmawati", "Puspita", "Sari", "Dewi", "Kusuma",
    "Saetan", "Saechao", "Saelee", "Tang", "Wong", "Chan", "Ho", "Cheung",
    "Nguyen", "Tran", "Le", "Pham", "Hoang", "Huynh", "Phan", "Vu",
    "Vo", "Dang", "Bui", "Do", "Ho", "Ngo", "Duong", "Ly",
    "Hernandez", "Flores", "Reyes", "Cruz", "Morales", "Gomez", "Ramos", "Mendoza",
    "Castillo", "Chavez", "Vargas", "Rivera", "Ortega", "Delgado", "Guerrero", "Contreras",
    "Campbell", "Stewart", "Robertson", "Thomson", "Reid", "Ross", "Paterson", "Fraser",
    "Hughes", "Evans", "Thomas", "Roberts", "Jones", "Williams", "Davies", "Lewis",
    "Novak", "Svoboda", "Novotny", "Dvorak", "Cerny", "Prochazka", "Kucera", "Vesely",
    "Horvath", "Kovacs", "Toth", "Szabo", "Nagy", "Varga", "Kiss", "Molnar",
    "Korhonen", "Virtanen", "Makinen", "Nieminen", "Makela", "Hamalainen", "Laine", "Heikkinen",
    "Koskinen", "Jarvinen", "Lehtonen", "Lehtinen", "Saarinen", "Salminen", "Heinonen", "Niemi",
    "Kovalenko", "Bondarenko", "Tkachenko", "Kravchenko", "Oliynyk", "Shevchenko", "Polischuk", "Bondar",
    "Horvat", "Kovacevic", "Babic", "Maric", "Juric", "Novakovic", "Knezevic", "Pavlovic",
    "Kazlauskas", "Jankauskas", "Petrauskas", "Stankevicius", "Vasiliauskas", "Zukauskas", "Butkus", "Paulauskas",
    "Berzins", "Kalnins", "Ozolins", "Liepins", "Jansons", "Ozols", "Kruminsh", "Birkavs",
    "Jensen", "Nielsen", "Hansen", "Pedersen", "Andersen", "Christensen", "Larsen", "Sorensen",
    "Johansson", "Andersson", "Karlsson", "Nilsson", "Eriksson", "Larsson", "Olsson", "Persson",
    "Tamm", "Saar", "Sepp", "Kask", "Kukk", "Ilves", "Magi", "Rebane",
    "Novak", "Horvat", "Kovacic", "Krajnc", "Zupan", "Potocnik", "Bizjak", "Vidmar",
    "Markovic", "Nikolic", "Jovanovic", "Djordjevic", "Stojanovic", "Ilic", "Stankovic", "Pavlovic",
    "Ivanov", "Georgiev", "Dimitrov", "Petrov", "Nikolov", "Hristov", "Stoyanov", "Todorov",
    "Popa", "Popescu", "Radu", "Dumitru", "Stan", "Stoica", "Gheorghe", "Matei",
    "Ben Ali", "Trabelsi", "Khelifi", "Gharbi", "Mejri", "Mansouri", "Chaabane", "Hammami",
    "El Amrani", "Bennani", "Tazi", "Alaoui", "Fassi", "Berrada", "Mansouri", "Idrissi",
    "Abdelrahman", "Elsayed", "Abdelaziz", "Soliman", "Salama", "Fahmy", "Gaber", "Khalil",
    "Al-Rashid", "Al-Saud", "Al-Faisal", "Al-Sulaiman", "Al-Abdullah", "Al-Khalid", "Al-Salem", "Al-Hassan",
    "Al-Maktoum", "Al-Nahyan", "Al-Qasimi", "Al-Nuaimi", "Al-Mualla", "Al-Sharqi", "Al-Falasi", "Al-Suwaidi",
    "Al-Musawi", "Al-Sadr", "Al-Hakim", "Al-Maliki", "Al-Jubouri", "Al-Dulaimi", "Al-Tamimi", "Al-Khatib",
    "Washington", "Jefferson", "Roosevelt", "Lincoln", "Monroe", "Madison", "Jackson", "Adams",
    "Quintero", "Salgado", "Montoya", "Cardenas", "Pineda", "Valdez", "Aguirre", "Cabrera",
    "Rojas", "Guzman", "Pena", "Rios", "Mejia", "Navas", "Solis", "Carrillo",
    "Bello", "Lugo", "Vega", "Mora", "Pacheco", "Campos", "Arias", "Leon",
    "Cohen", "Levi", "Mizrahi", "Biton", "Dahan", "Avraham", "Yosef", "Peretz",
    "Sherpa", "Tamang", "Gurung", "Magar", "Rai", "Limbu", "Thapa", "Adhikari",
    "Silva", "Perera", "Fernando", "De Silva", "Bandara", "Senanayake", "Jayasuriya", "Wijesinghe",
    "Diallo", "Barry", "Camara", "Soumah", "Sylla", "Keita", "Konate", "Traore",
    "Sow", "Ba", "Diop", "Fall", "Ndiaye", "Gueye", "Thiam", "Sarr",
    "Kouassi", "Kone", "Bamba", "Diarra", "Soro", "Yao", "Koffi", "Ouattara",
    "Moyo", "Ncube", "Dube", "Sibanda", "Mpofu", "Tshuma", "Nyoni", "Mlilo",
    "Sekgoma", "Mokgweetsi", "Kgafela", "Khama", "Masire", "Mogae", "Kgamanyane", "Pilane",
    "Zulu", "Chileshe", "Banda", "Phiri", "Tembo", "Mwale", "Mwanza", "Lung",
    "Mukasa", "Kato", "Wasswa", "Ssemakula", "Lubega", "Nakato", "Namukasa", "Nalwanga",
]

# Deduplicate while preserving order and trim/pad cleanly to 1,000 entries each
FIRST_NAMES_1000 = list(dict.fromkeys(
    n.strip() for n in FIRST_NAMES_1000 if n.strip() and "frece" not in n.strip().lower()
))
while len(FIRST_NAMES_1000) < 1000:
    FIRST_NAMES_1000.append(f"Amara{len(FIRST_NAMES_1000) + 1}")
FIRST_NAMES_1000 = FIRST_NAMES_1000[:1000]

LAST_NAMES_1000 = list(dict.fromkeys(
    n.strip() for n in LAST_NAMES_1000 if n.strip() and "frece" not in n.strip().lower()
))
while len(LAST_NAMES_1000) < 1000:
    LAST_NAMES_1000.append(f"Bello{len(LAST_NAMES_1000) + 1}")
LAST_NAMES_1000 = LAST_NAMES_1000[:1000]


def generate_random_english_name() -> tuple[str, str]:
    return random.choice(FIRST_NAMES_1000), random.choice(LAST_NAMES_1000)


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


def db_add_group(chat_id: int):
    """Track group chat IDs in memory and Supabase if available."""
    known_group_ids.add(chat_id)
    if not supabase:
        return
    try:
        supabase.table("groups").upsert({"chat_id": chat_id}, on_conflict="chat_id").execute()
    except Exception as e:
        logging.debug(f"Group table upsert skipped/failed: {e}")


def db_get_all_groups() -> list[int]:
    """Retrieve all known group chat IDs."""
    groups = set(known_group_ids)
    if CHANNEL_CHAT_ID and CHANNEL_CHAT_ID < 0:
        groups.discard(CHANNEL_CHAT_ID)
    if supabase:
        try:
            response = supabase.table("groups").select("chat_id").execute()
            if response.data:
                for row in response.data:
                    if row.get("chat_id"):
                        groups.add(int(row["chat_id"]))
        except Exception as e:
            logging.debug(f"Group table select skipped/failed: {e}")
    return list(groups)


def db_get_managed_ranges(provider: str = None) -> list:
    if not supabase:
        if provider:
            return [r for r in DEFAULT_RANGES_ZEBRA if r.get("provider", "zebrasms") == provider]
        return list(DEFAULT_RANGES_ZEBRA)
    try:
        query = supabase.table("managed_ranges_zebra").select("*")
        if provider:
            query = query.eq("provider", provider)
        response = query.execute()
        return response.data if response.data else []
    except Exception as e:
        logging.error(f"Error fetching ranges from Supabase: {e}")
        return []


def db_get_all_managed_ranges() -> list:
    """Retrieve all managed ranges across providers."""
    if not supabase:
        return list(DEFAULT_RANGES_ZEBRA)
    try:
        response = supabase.table("managed_ranges_zebra").select("*").execute()
        return response.data if response.data else []
    except Exception as e:
        logging.error(f"Error fetching all ranges from Supabase: {e}")
        return []


def db_add_managed_range(service: str, country: str, flag: str, range_val: str, provider: str) -> bool:
    if not supabase:
        DEFAULT_RANGES_ZEBRA.append({
            "service": service,
            "country": country,
            "flag": flag,
            "range": range_val,
            "provider": provider,
        })
        return True
    try:
        data = {
            "service": service,
            "country": country,
            "flag": flag,
            "range": range_val,
            "provider": provider,
        }
        supabase.table("managed_ranges_zebra").upsert(data, on_conflict="service, range").execute()
        return True
    except Exception as e:
        logging.error(f"Error inserting range to Supabase: {e}")
        return False


def db_delete_specific_range(range_val: str, service: str = None):
    if not supabase:
        DEFAULT_RANGES_ZEBRA[:] = [
            r for r in DEFAULT_RANGES_ZEBRA
            if not (r.get("range") == range_val and (not service or r.get("service") == service))
        ]
        return
    try:
        query = supabase.table("managed_ranges_zebra").delete().eq("range", range_val)
        if service:
            query = query.eq("service", service)
        query.execute()
    except Exception as e:
        logging.error(f"Error deleting range from Supabase: {e}")


def db_clear_managed_ranges(provider: str):
    if not supabase:
        DEFAULT_RANGES_ZEBRA[:] = [
            r for r in DEFAULT_RANGES_ZEBRA if r.get("provider", "zebrasms") != provider
        ]
        return
    try:
        supabase.table("managed_ranges_zebra").delete().eq("provider", provider).execute()
    except Exception as e:
        logging.error(f"Error clearing ranges from Supabase: {e}")


def invalidate_liveaccess_cache():
    """Force immediate re-merge on the next range lookup without discarding last good live rows."""
    _liveaccess_cache["updated_at"] = 0.0


def find_matching_db_range(phone_number: str, db_ranges: list[dict]) -> dict | None:
    """Checks whether an incoming phone number matches a range saved in the database."""
    target_digits = re.sub(r"\D", "", phone_number or "")
    if not target_digits:
        return None
    for row in db_ranges:
        rng_raw = (row.get("range") or "").strip()
        prefix_digits = re.sub(r"\D", "", rng_raw)
        if prefix_digits and target_digits.startswith(prefix_digits):
            return row
    return None


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
        return f"{digits[:3]}{digits[3:6]}ⒶⓃⓊ{digits[-2:]}"
    elif len(digits) >= 6:
        return f"{digits[:3]}ANU{digits[-2:]}"
    return phone


def format_otp_detail_line(sender: str, msg_text: str) -> str:
    """
    Replaces raw full-message text with a clean status/client label:
    - Facebook messages with hash 'Laz+nxCarLW' -> 'FB OFFICIAL APK'
    - Facebook messages with hash 'H29Q+Fsn4Sr' -> 'FB lite apk'
    - Other Facebook code messages -> 'FB Web Version'
    - Non-Facebook messages -> clean single-line summary without raw message dump
    """
    raw = (msg_text or "").strip()
    lower_msg = raw.lower()
    lower_sender = (sender or "").strip().lower()

    is_facebook = ("facebook" in lower_msg) or ("fb" in lower_sender) or ("facebook" in lower_sender)
    if is_facebook:
        if "laz+nxcarlw" in lower_msg:
            return "FB OFFICIAL APK"
        if "h29q+fsn4sr" in lower_msg:
            return "FB lite apk"
        if "code" in lower_msg or "<#>" in lower_msg or is_facebook:
            return "FB Web Version"

    cleaned = " ".join(raw.split())
    return cleaned if cleaned else "OTP Received"


def extract_code(message_text: str) -> str:
    match = re.search(r"\b\d{4,8}\b", message_text or "")
    return match.group(0) if match else "No code found"


# --- ZebraSMS API Client ---

class ZebraSMSClient:
    def __init__(self, api_key: str):
        self.base_url = ZEBRASMS_BASE_URL.rstrip("/")
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
        try:
            params = {"sender": sender} if sender else {}
            response = await self.client.get(
                f"{self.base_url}/publicapi/liveaccess",
                headers=self.headers,
                params=params,
            )
            if response.status_code != 200:
                logging.warning(f"[zebrasms liveaccess] HTTP {response.status_code}: {response.text[:200]}")
                return {"meta": {"code": -500, "error": f"HTTP {response.status_code}"}}
            data = response.json()
            if not isinstance(data, dict) or data.get("meta", {}).get("code") != 0:
                logging.warning(f"[zebrasms liveaccess] Non-zero meta: {data}")
            return data
        except Exception as e:
            logging.error(f"[zebrasms liveaccess] Request error: {e}")
            return {"meta": {"code": -500, "error": str(e)}}

    async def get_number(self, range_val: str) -> dict:
        try:
            payload = {"range": range_val}
            response = await self.client.post(
                f"{self.base_url}/publicapi/getnum",
                headers=self.headers,
                json=payload,
            )
            print(f"ZebraSMS getnum Response [{response.status_code}]: {response.text}")
            if response.status_code != 200:
                return {"meta": {"code": -500, "error": f"HTTP {response.status_code}"}}
            return response.json()
        except Exception as e:
            print(f"ZebraSMS getnum Exception: {e}")
            return {"meta": {"code": -500, "error": str(e)}}

    async def get_updates(self) -> dict:
        try:
            response = await self.client.get(
                f"{self.base_url}/publicapi/getupdate",
                headers=self.headers,
            )
            if response.status_code != 200:
                return {"meta": {"code": -500, "error": f"HTTP {response.status_code}"}}
            return response.json()
        except Exception as e:
            return {"meta": {"code": -500, "error": str(e)}}


# --- MK Network API v3 Client ---

class MKNetworkClient:
    def __init__(self, api_key: str):
        self.base_url = MKNETWORK_BASE_URL.rstrip("/")
        self.headers = {
            "mknetwork-key": api_key,
            "Content-Type": "application/json",
        }
        self.client = httpx.AsyncClient(
            follow_redirects=True,
            timeout=6.0,
            limits=httpx.Limits(max_keepalive_connections=50, max_connections=200),
        )

    async def get_access_list(self, limit: int = 200, offset: int = 0) -> dict:
        try:
            response = await self.client.get(
                f"{self.base_url}/access_list",
                headers=self.headers,
                params={"limit": limit, "offset": offset},
            )
            if response.status_code != 200:
                logging.warning(f"[mknetwork access_list] HTTP {response.status_code}: {response.text[:200]}")
                return {"meta": {"code": response.status_code, "status": "error"}}
            return response.json()
        except Exception as e:
            logging.error(f"[mknetwork access_list] Request error: {e}")
            return {"meta": {"code": -500, "status": "error", "error": str(e)}}

    async def get_number(self, range_val: str) -> dict:
        try:
            payload = {"range": range_val}
            response = await self.client.post(
                f"{self.base_url}/getnum/number",
                headers=self.headers,
                json=payload,
            )
            print(f"MKNetwork getnum Response [{response.status_code}]: {response.text}")
            if response.status_code != 200:
                return {"meta": {"code": response.status_code, "status": "error"}}
            return response.json()
        except Exception as e:
            print(f"MKNetwork getnum Exception: {e}")
            return {"meta": {"code": -500, "status": "error", "error": str(e)}}

    async def check_status(self, request_ids: list[int]) -> dict:
        if not request_ids:
            return {"meta": {"code": 200, "status": "success"}, "data": {"results": []}}
        try:
            payload = {"request_ids": request_ids[:100]}
            response = await self.client.post(
                f"{self.base_url}/check/status",
                headers=self.headers,
                json=payload,
            )
            if response.status_code != 200:
                logging.warning(f"[mknetwork check/status] HTTP {response.status_code}: {response.text[:200]}")
                return {"meta": {"code": response.status_code, "status": "error"}}
            return response.json()
        except Exception as e:
            logging.error(f"[mknetwork check/status] Request error: {e}")
            return {"meta": {"code": -500, "status": "error", "error": str(e)}}


# --- VoltxSMS (3rd Provider) Client ---

class VoltxSMSClient:
    def __init__(self, api_key: str, base_url: str = VOLTXSMS_BASE_URL):
        self.base_url = (base_url or VOLTXSMS_BASE_URL).rstrip("/")
        self.headers = {
            "mauthapi": api_key,
            "Content-Type": "application/json",
        }
        self.client = httpx.AsyncClient(
            follow_redirects=True,
            timeout=6.0,
            limits=httpx.Limits(max_keepalive_connections=50, max_connections=200),
        )

    @staticmethod
    def _normalize_rid(range_val: str) -> str:
        raw = (range_val or "").strip()
        if not raw:
            return ""
        cleaned = re.sub(r"[xX]+$", "", raw).strip()
        return cleaned if cleaned else raw

    async def get_live_access(self) -> dict:
        try:
            response = await self.client.get(
                f"{self.base_url}/liveaccess",
                headers=self.headers,
            )
            if response.status_code != 200:
                logging.warning(f"[voltxsms liveaccess] HTTP {response.status_code}: {response.text[:200]}")
                return {"meta": {"code": response.status_code, "status": "error"}}
            return response.json()
        except Exception as e:
            logging.error(f"[voltxsms liveaccess] Request error: {e}")
            return {"meta": {"code": -500, "status": "error", "error": str(e)}}

    async def get_number(self, range_val: str) -> dict:
        try:
            rid = self._normalize_rid(range_val)
            payload = {"rid": rid}
            response = await self.client.post(
                f"{self.base_url}/getnum",
                headers=self.headers,
                json=payload,
            )
            print(f"VoltxSMS getnum Response [{response.status_code}]: {response.text}")
            if response.status_code != 200:
                return {"meta": {"code": response.status_code, "status": "error"}}
            return response.json()
        except Exception as e:
            print(f"VoltxSMS getnum Exception: {e}")
            return {"meta": {"code": -500, "status": "error", "error": str(e)}}

    async def get_success_otp(self) -> dict:
        try:
            response = await self.client.get(
                f"{self.base_url}/success-otp",
                headers=self.headers,
            )
            if response.status_code != 200:
                logging.warning(f"[voltxsms success-otp] HTTP {response.status_code}: {response.text[:200]}")
                return {"meta": {"code": response.status_code, "status": "error"}}
            return response.json()
        except Exception as e:
            logging.error(f"[voltxsms success-otp] Request error: {e}")
            return {"meta": {"code": -500, "status": "error", "error": str(e)}}

    async def get_console(self) -> dict:
        try:
            response = await self.client.get(
                f"{self.base_url}/console",
                headers=self.headers,
            )
            if response.status_code != 200:
                logging.warning(f"[voltxsms console] HTTP {response.status_code}: {response.text[:200]}")
                return {"meta": {"code": response.status_code, "status": "error"}}
            return response.json()
        except Exception as e:
            logging.error(f"[voltxsms console] Request error: {e}")
            return {"meta": {"code": -500, "status": "error", "error": str(e)}}


zebrasms_client = ZebraSMSClient(ZEBRASMS_API_KEY) if ZEBRASMS_API_KEY else None
mknetwork_client = MKNetworkClient(MKNETWORK_API_KEY) if MKNETWORK_API_KEY else None
voltxsms_client = VoltxSMSClient(VOLTXSMS_API_KEY, VOLTXSMS_BASE_URL) if VOLTXSMS_API_KEY else None


async def _build_live_and_managed_ranges() -> list:
    """
    Combines saved DB/fallback ranges with live ranges from ZebraSMS, MK Network v3, and VoltxSMS:
    - Keeps both DB managed ranges and live auto-ranges without DB ranges starving live slots.
    - Retains last known live rows if a single API poll fails or returns empty.
    - Keeps exact sender casing (does not combine 'FACEBOOK' and 'Facebook').
    - Ignores duplicate ranges for the same sender ID.
    - Caps live auto-ranges at 10 per sender ID while keeping saved DB ranges visible.
    """
    per_sender_ranges = {}
    per_sender_seen = {}

    def add_range_entry(
        srv: str,
        rng: str,
        country: str,
        flag: str,
        provider: str,
        from_db: bool,
        enforce_cap: bool = True,
    ) -> bool:
        srv_clean = (srv or "").strip()
        rng_clean = (rng or "").strip()
        if not srv_clean or not rng_clean:
            return False
        if not from_db and not is_allowed_social_sender(srv_clean):
            return False

        per_sender_ranges.setdefault(srv_clean, [])
        per_sender_seen.setdefault(srv_clean, set())

        if rng_clean in per_sender_seen[srv_clean]:
            return False

        if enforce_cap:
            live_count = sum(1 for item in per_sender_ranges[srv_clean] if not item.get("from_db"))
            if live_count >= 10:
                return False

        c_name, detected_flag = auto_detect_country_and_flag(country, rng_clean)
        per_sender_seen[srv_clean].add(rng_clean)
        per_sender_ranges[srv_clean].append({
            "service": srv_clean,
            "country": c_name,
            "flag": flag if flag and flag != "🌐" else detected_flag,
            "range": rng_clean,
            "provider": provider or "zebrasms",
            "from_db": from_db,
        })
        return True

    # 1. Saved DB / fallback ranges
    managed_rows = await asyncio.to_thread(db_get_all_managed_ranges)
    for r in managed_rows:
        add_range_entry(
            srv=r.get("service", ""),
            rng=r.get("range", ""),
            country=r.get("country", ""),
            flag=r.get("flag", ""),
            provider=r.get("provider", "zebrasms"),
            from_db=True,
            enforce_cap=False,
        )

    # 2. Fetch live ranges from ZebraSMS (/publicapi/liveaccess)
    zebra_live_rows = list(_liveaccess_cache.get("zebra_live_rows") or [])
    if zebrasms_client:
        res = await zebrasms_client.get_live_access()
        if isinstance(res, dict) and res.get("meta", {}).get("code") == 0:
            fetched_rows = res.get("data", {}).get("rows", []) or []
            if fetched_rows:
                zebra_live_rows = fetched_rows
                _liveaccess_cache["zebra_live_rows"] = fetched_rows
            elif zebra_live_rows:
                logging.warning("[zebrasms liveaccess] Empty rows returned; preserving previous live ranges.")
        elif zebra_live_rows:
            logging.warning("[zebrasms liveaccess] API error/rate-limit; preserving previous live ranges.")

    live_added = 0
    for row in zebra_live_rows:
        srv = (row.get("sender") or "").strip()
        if not srv or not is_allowed_social_sender(srv):
            continue
        for rng in (row.get("ranges") or []):
            if add_range_entry(
                srv=srv,
                rng=str(rng),
                country="",
                flag="",
                provider="zebrasms",
                from_db=False,
                enforce_cap=True,
            ):
                live_added += 1

    # 3. Fetch live ranges from MK Network v3 (/access_list)
    mk_items = list(_liveaccess_cache.get("mk_access_items") or [])
    if mknetwork_client:
        mk_res = await mknetwork_client.get_access_list(limit=200, offset=0)
        if isinstance(mk_res, dict) and mk_res.get("meta", {}).get("code") == 200:
            raw_data = mk_res.get("data", {})
            if isinstance(raw_data, list):
                fetched_mk = raw_data
            elif isinstance(raw_data, dict):
                fetched_mk = raw_data.get("rows") or raw_data.get("items") or raw_data.get("ranges") or []
            else:
                fetched_mk = []
            if fetched_mk:
                mk_items = fetched_mk
                _liveaccess_cache["mk_access_items"] = fetched_mk
            elif mk_items:
                logging.warning("[mknetwork access_list] Empty items returned; preserving previous MK live ranges.")
        elif mk_items:
            logging.warning("[mknetwork access_list] API error/rate-limit; preserving previous MK live ranges.")

    for item in mk_items:
        if not isinstance(item, dict):
            continue
        srv = (item.get("sender") or item.get("service") or item.get("operator") or "").strip()
        country = (item.get("country") or "").strip()
        rng_list = item.get("ranges")
        if isinstance(rng_list, list) and rng_list:
            for rng in rng_list:
                if srv and is_allowed_social_sender(srv):
                    if add_range_entry(
                        srv=srv,
                        rng=str(rng),
                        country=country,
                        flag="",
                        provider="mknetwork",
                        from_db=False,
                        enforce_cap=True,
                    ):
                        live_added += 1
        else:
            rng_single = str(item.get("range") or item.get("prefix") or "").strip()
            if srv and rng_single and is_allowed_social_sender(srv):
                if add_range_entry(
                    srv=srv,
                    rng=rng_single,
                    country=country,
                    flag="",
                    provider="mknetwork",
                    from_db=False,
                    enforce_cap=True,
                ):
                    live_added += 1

    # 4. Fetch live ranges from VoltxSMS (/liveaccess -> data.services[].ranges)
    voltx_services = list(_liveaccess_cache.get("voltx_live_services") or [])
    if voltxsms_client:
        vx_res = await voltxsms_client.get_live_access()
        if isinstance(vx_res, dict) and vx_res.get("meta", {}).get("code") == 200:
            raw_vx_data = vx_res.get("data", {})
            if isinstance(raw_vx_data, dict):
                fetched_vx = raw_vx_data.get("services") or raw_vx_data.get("rows") or []
            elif isinstance(raw_vx_data, list):
                fetched_vx = raw_vx_data
            else:
                fetched_vx = []
            if fetched_vx:
                voltx_services = fetched_vx
                _liveaccess_cache["voltx_live_services"] = fetched_vx
            elif voltx_services:
                logging.warning("[voltxsms liveaccess] Empty services returned; preserving previous Voltx live ranges.")
        elif voltx_services:
            logging.warning("[voltxsms liveaccess] API error/rate-limit; preserving previous Voltx live ranges.")

    for svc_item in voltx_services:
        if not isinstance(svc_item, dict):
            continue
        srv = (svc_item.get("sid") or svc_item.get("sender") or svc_item.get("service") or "").strip()
        country = (svc_item.get("country") or "").strip()
        rng_list = svc_item.get("ranges") or svc_item.get("rangas") or []
        if isinstance(rng_list, list) and rng_list:
            for rng in rng_list:
                if srv and is_allowed_social_sender(srv):
                    if add_range_entry(
                        srv=srv,
                        rng=str(rng),
                        country=country,
                        flag="",
                        provider="voltxsms",
                        from_db=False,
                        enforce_cap=True,
                    ):
                        live_added += 1
        else:
            rng_single = str(svc_item.get("range") or svc_item.get("rid") or "").strip()
            if srv and rng_single and is_allowed_social_sender(srv):
                if add_range_entry(
                    srv=srv,
                    rng=rng_single,
                    country=country,
                    flag="",
                    provider="voltxsms",
                    from_db=False,
                    enforce_cap=True,
                ):
                    live_added += 1

    logging.info(
        f"[ranges] zebra_rows={len(zebra_live_rows)}, mk_items={len(mk_items)}, "
        f"voltx_services={len(voltx_services)}, db_rows={len(managed_rows)}, live_added={live_added}"
    )

    combined = []
    for ranges_list in per_sender_ranges.values():
        combined.extend(ranges_list)

    if combined:
        _liveaccess_cache["ranges"] = combined
        _liveaccess_cache["updated_at"] = time.monotonic()

    return combined


async def get_live_and_managed_ranges(force_refresh: bool = False) -> list:
    """Returns cached live + managed ranges, refreshing when stale or using stale cache on transient errors."""
    now = time.monotonic()
    if (
        not force_refresh
        and _liveaccess_cache["ranges"]
        and (now - _liveaccess_cache["updated_at"]) < LIVEACCESS_CACHE_TTL_SECONDS
    ):
        return _liveaccess_cache["ranges"]

    async with _liveaccess_lock:
        now = time.monotonic()
        if (
            not force_refresh
            and _liveaccess_cache["ranges"]
            and (now - _liveaccess_cache["updated_at"]) < LIVEACCESS_CACHE_TTL_SECONDS
        ):
            return _liveaccess_cache["ranges"]

        fresh = await _build_live_and_managed_ranges()
        if fresh:
            return fresh
        if _liveaccess_cache["ranges"]:
            logging.warning("[liveaccess] Falling back to cached ranges after empty/error response.")
            return _liveaccess_cache["ranges"]
        return fresh


async def liveaccess_auto_refresh_loop():
    """Periodically refreshes auto-ranging cache in the background."""
    while True:
        try:
            await get_live_and_managed_ranges(force_refresh=True)
        except Exception as e:
            logging.error(f"Error in liveaccess auto-refresh loop: {e}")
        await asyncio.sleep(LIVEACCESS_REFRESH_INTERVAL_SECONDS)


async def build_channel_keyboard(bot, otp_code: str, include_bot_button: bool = True) -> InlineKeyboardMarkup:
    bot_info = await bot.get_me()
    bot_username = (bot_info.username or "").lstrip("@")

    first_row = [
        InlineKeyboardButton(f"📋 Copy OTP: {otp_code}", copy_text=CopyTextButton(text=str(otp_code)))
    ]
    if include_bot_button and bot_username:
        first_row.append(InlineKeyboardButton("🤖 Bot", url=f"https://t.me/{bot_username}"))

    return InlineKeyboardMarkup([
        first_row,
        [InlineKeyboardButton("👨‍💻 Developer", url="https://t.me/anstans")],
    ])


async def auto_delete_allocation_message(context: ContextTypes.DEFAULT_TYPE, chat_id: int, message_id: int, alloc_key: str):
    """Waits 15 minutes (900 seconds) and deletes the message if no OTP arrived."""
    await asyncio.sleep(900)
    if alloc_key in active_allocations:
        active_allocations.pop(alloc_key, None)
        try:
            await context.bot.delete_message(chat_id=chat_id, message_id=message_id)
        except Exception as e:
            logging.error(f"Error auto-deleting message after 15 min: {e}")


async def _safe_send_broadcast_message(
    bot,
    chat_id: int,
    text: str,
    reply_markup=None,
) -> bool:
    """Sends a broadcast message safely while respecting Telegram rate limits and blocked users."""
    for attempt in range(2):
        try:
            await bot.send_message(
                chat_id=chat_id,
                text=text,
                parse_mode="Markdown",
                reply_markup=reply_markup,
            )
            return True
        except RetryAfter as err:
            await asyncio.sleep(float(err.retry_after) + 0.5)
        except Forbidden:
            return False
        except Exception as e:
            if attempt == 0:
                await asyncio.sleep(0.2)
            else:
                logging.debug(f"Broadcast send error to {chat_id}: {e}")
                return False
    return False


async def maybe_broadcast_db_range_country_alert(
    app,
    target_number: str,
    country_name: str,
    flag_icon: str,
    sender: str,
    otp_code: str,
    msg_text: str,
):
    """
    When an OTP matches a DB-saved range and no alert has been sent for that country
    in the last 30 minutes (or it is a new country), broadcasts to all subscribers,
    CHANNEL_CHAT_ID, and tracked groups.
    """
    db_ranges = await asyncio.to_thread(db_get_all_managed_ranges)
    matched_db_range = find_matching_db_range(target_number, db_ranges)
    if not matched_db_range:
        return

    resolved_country, resolved_flag = auto_detect_country_and_flag(
        matched_db_range.get("country") or country_name,
        target_number,
    )
    if matched_db_range.get("flag") and matched_db_range.get("flag") != "🌐":
        resolved_flag = matched_db_range["flag"]
    elif flag_icon and flag_icon != "🌐":
        resolved_flag = flag_icon

    country_key = resolved_country.strip().lower()
    if not country_key or country_key == "unknown":
        return

    now = time.monotonic()
    async with _country_broadcast_lock:
        last_sent = last_country_broadcast_at.get(country_key, 0.0)
        if last_sent and (now - last_sent) < COUNTRY_BROADCAST_COOLDOWN_SECONDS:
            return
        last_country_broadcast_at[country_key] = now

    masked_num = mask_phone_number(target_number)
    db_range_val = (matched_db_range.get("range") or "").strip()
    db_provider = (matched_db_range.get("provider") or "zebrasms").strip()
    sender_label = (sender or "OTP").strip()
    country_label = (resolved_country or "Unknown").strip()
    flag_label = (resolved_flag or "🌐").strip()
    code_label = str(otp_code or "").strip()

    col1_w = 8
    alert_table_block = "\n".join([
        f"🌐 {'COUNTRY':<{col1_w}} | {flag_label} {country_label}",
        f"🔢 {'RANGE':<{col1_w}} | {db_range_val}",
        f"» {'NUMBER':<{col1_w}} | {masked_num}",
        f"👤 {'SENDER':<{col1_w}} | {sender_label}",
        f"❤ {'CODE':<{col1_w}} | {code_label}",
    ])
    alert_detail_line = format_otp_detail_line(sender_label, msg_text)

    alert_text = (
        "🌍 *NEW COUNTRY 30 min* 🌍\n"
        f"```text\n{alert_table_block}\n```\n"
        f"✅ `{alert_detail_line}`"
    )
    alert_keyboard = InlineKeyboardMarkup([
        [
            InlineKeyboardButton(
                f"📱 Get Number ({db_range_val})",
                callback_data=f"prov_{db_provider}_{db_range_val}",
            )
        ]
    ])

    target_chats: list[int] = []
    seen_targets: set[int] = set()

    if CHANNEL_CHAT_ID:
        target_chats.append(CHANNEL_CHAT_ID)
        seen_targets.add(CHANNEL_CHAT_ID)

    for gid in await asyncio.to_thread(db_get_all_groups):
        if gid not in seen_targets:
            target_chats.append(gid)
            seen_targets.add(gid)

    for uid in await asyncio.to_thread(db_get_all_users):
        if uid not in seen_targets:
            target_chats.append(uid)
            seen_targets.add(uid)

    for chat_target in target_chats:
        await _safe_send_broadcast_message(
            bot=app.bot,
            chat_id=chat_target,
            text=alert_text,
            reply_markup=alert_keyboard,
        )
        await asyncio.sleep(0.04)


async def cmd_test_sms(update: Update, context: ContextTypes.DEFAULT_TYPE):
    dummy_number = "+225011234567"
    dummy_sender = "FB NEW"
    dummy_message = "<#> 07520 is your Facebook code\nLaz+nxCarLW"
    country_name, flag_icon = auto_detect_country_and_flag("Ivory Coast", dummy_number)

    code = extract_code(dummy_message)
    masked_num = mask_phone_number(dummy_number)

    dm_text = f"📱 `{dummy_number}`"
    copy_keyboard = InlineKeyboardMarkup([
        [InlineKeyboardButton(f"{code}", copy_text=CopyTextButton(text=str(code)))]
    ])
    await update.message.reply_text(dm_text, parse_mode="Markdown", reply_markup=copy_keyboard)

    if CHANNEL_CHAT_ID:
        sender_label = (dummy_sender or "OTP").strip()
        country_label = (country_name or "Unknown").strip()
        flag_label = (flag_icon or "🌐").strip()
        code_label = str(code or "").strip()
        col1_w = 8
        channel_table_block = "\n".join([
            f"» {'NUMBER':<{col1_w}} | {masked_num}",
            f"🌐 {'COUNTRY':<{col1_w}} | {flag_label} {country_label}",
            f"❤ {'CODE':<{col1_w}} | {code_label}",
        ])
        channel_detail_line = format_otp_detail_line(sender_label, dummy_message)
        channel_text = (
            f"📢 *{sender_label} • OTP • RCV*\n"
            f"```text\n{channel_table_block}\n```\n"
            f"✅ `{channel_detail_line}`"
        )
        channel_keyboard = await build_channel_keyboard(context.bot, code, include_bot_button=True)
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

    asyncio.create_task(
        forward_otp_to_groups(
            app=context.application,
            target_number=dummy_number,
            country_name=country_name,
            flag_icon=flag_icon,
            sender=dummy_sender,
            otp_code=code,
            msg_text=dummy_message,
        )
    )


async def forward_otp_to_groups(
    app,
    target_number: str,
    country_name: str,
    flag_icon: str,
    sender: str,
    otp_code: str,
    msg_text: str,
):
    """Forwards received OTPs to tracked/configured groups in a clean table-like monospace layout matching the screenshot."""
    group_chats: list[int] = []
    seen_groups: set[int] = set()

    for gid in await asyncio.to_thread(db_get_all_groups):
        if gid and gid not in seen_groups:
            group_chats.append(gid)
            seen_groups.add(gid)

    if not group_chats:
        return

    masked_num = mask_phone_number(target_number)
    sender_label = (sender or "OTP").strip()
    country_label = (country_name or "Unknown").strip()
    flag_label = (flag_icon or "🌐").strip()
    code_label = str(otp_code or "").strip()
    sms_body = (msg_text or "").strip()

    # Align labels & values in a monospace table block so it renders cleanly on mobile Telegram
    col1_w = 8
    table_lines = [
        f"» {'NUMBER':<{col1_w}} | {masked_num}",
        f"🌐 {'COUNTRY':<{col1_w}} | {flag_label} {country_label}",
        f"❤ {'CODE':<{col1_w}} | {code_label}",
    ]
    table_block = "\n".join(table_lines)

    detail_line = format_otp_detail_line(sender_label, sms_body)

    group_text = (
        f"⚡ *{sender_label} • OTP • RCV* ⚡\n"
        f"```text\n{table_block}\n```\n"
        f"✅ `{detail_line}`"
    )

    bot_username = await get_cached_bot_username(app.bot)
    buttons_row_1 = []
    if CHANNEL_URL and CHANNEL_URL != "https://t.me/":
        buttons_row_1.append(InlineKeyboardButton("↗ CHANNEL", url=CHANNEL_URL))
    if bot_username:
        buttons_row_1.append(InlineKeyboardButton("↗ NUMBER", url=f"https://t.me/{bot_username}"))

    keyboard_rows = []
    if buttons_row_1:
        keyboard_rows.append(buttons_row_1)
    keyboard_rows.append([
        InlineKeyboardButton(f"📋 Copy OTP: {code_label}", copy_text=CopyTextButton(text=code_label))
    ])
    group_keyboard = InlineKeyboardMarkup(keyboard_rows)

    for gid in group_chats:
        await _safe_send_broadcast_message(
            bot=app.bot,
            chat_id=gid,
            text=group_text,
            reply_markup=group_keyboard,
        )
        await asyncio.sleep(0.04)


# --- Non-Blocking Dispatch Consumer Queue ---
async def dispatch_consumer_loop(app):
    """Permanently drains the queue and sends messages instantly."""
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

            dm_text = f"📱 `{target_number}`"

            dm_keyboard = InlineKeyboardMarkup([
                [InlineKeyboardButton(f"{otp_code}", copy_text=CopyTextButton(text=str(otp_code)))]
            ])

            try:
                await app.bot.send_message(chat_id=user_chat_id, text=dm_text, parse_mode="Markdown", reply_markup=dm_keyboard)
            except Exception as e:
                logging.error(f"Error sending DM: {e}")

            if CHANNEL_CHAT_ID:
                masked_num = mask_phone_number(target_number)
                sender_label = (sender or "OTP").strip()
                country_label = (country_name or "Unknown").strip()
                flag_label = (flag_icon or "🌐").strip()
                code_label = str(otp_code or "").strip()
                sms_body = (msg_text or "").strip()

                col1_w = 8
                channel_table_block = "\n".join([
                    f"» {'NUMBER':<{col1_w}} | {masked_num}",
                    f"🌐 {'COUNTRY':<{col1_w}} | {flag_label} {country_label}",
                    f"❤ {'CODE':<{col1_w}} | {code_label}",
                ])
                channel_detail_line = format_otp_detail_line(sender_label, sms_body)
                channel_text = (
                    f"📢 *{sender_label} • OTP • RCV*\n"
                    f"```text\n{channel_table_block}\n```\n"
                    f"✅ `{channel_detail_line}`"
                )
                channel_keyboard = await build_channel_keyboard(app.bot, otp_code, include_bot_button=True)
                try:
                    await app.bot.send_message(chat_id=CHANNEL_CHAT_ID, text=channel_text, parse_mode="Markdown", reply_markup=channel_keyboard)
                except Exception as err:
                    logging.error(f"Failed to post to channel: {err}")

            asyncio.create_task(
                forward_otp_to_groups(
                    app=app,
                    target_number=target_number,
                    country_name=country_name,
                    flag_icon=flag_icon,
                    sender=sender,
                    otp_code=otp_code,
                    msg_text=msg_text,
                )
            )

            asyncio.create_task(
                maybe_broadcast_db_range_country_alert(
                    app=app,
                    target_number=target_number,
                    country_name=country_name,
                    flag_icon=flag_icon,
                    sender=sender,
                    otp_code=otp_code,
                    msg_text=msg_text,
                )
            )

            otp_dispatch_queue.task_done()
        except Exception as e:
            logging.error(f"Error in consumer loop: {e}")
            await asyncio.sleep(0.1)


async def dispatch_otp(app, allocation_info, target_number, otp_code, sender, msg_text):
    """Pushes OTP details into the non-blocking queue instantly."""
    await otp_dispatch_queue.put((allocation_info, target_number, otp_code, sender, msg_text))


# --- Global Checker Loops polling ZebraSMS, MK Network v3, and VoltxSMS ---
async def global_zebrasms_poller(app):
    """Polls ZebraSMS /publicapi/getupdate endpoint and dispatches messages to matching active allocations."""
    while True:
        try:
            if zebrasms_client and active_allocations:
                res = await zebrasms_client.get_updates()
                if isinstance(res, dict) and res.get("meta", {}).get("code") == 0:
                    rows = res.get("data", {}).get("rows", []) or []
                    for row in rows:
                        target_num = row.get("number") or ""
                        msg_text = row.get("message", "")
                        sender = row.get("sender", "UNKNOWN")
                        otp_code = extract_code(msg_text)

                        for alloc_key, info in list(active_allocations.items()):
                            if info.get("provider", "zebrasms") != "zebrasms":
                                continue
                            alloc_num = info.get("number") or ""
                            if alloc_num == target_num or alloc_num.replace("+", "") == target_num.replace("+", ""):
                                await dispatch_otp(app, info, target_num, otp_code, sender, msg_text)
                                active_allocations.pop(alloc_key, None)
        except Exception as e:
            logging.error(f"Error in ZebraSMS global poller: {e}")

        await asyncio.sleep(2.0)


async def global_mknetwork_poller(app):
    """Polls MK Network v3 /check/status in batches for active MK allocations and deduplicates by sms_id."""
    while True:
        try:
            if mknetwork_client and active_allocations:
                mk_entries = [
                    (alloc_key, info)
                    for alloc_key, info in list(active_allocations.items())
                    if info.get("provider") == "mknetwork" and info.get("request_id") is not None
                ]
                if mk_entries:
                    id_to_alloc = {int(info["request_id"]): (alloc_key, info) for alloc_key, info in mk_entries}
                    request_ids = list(id_to_alloc.keys())

                    for idx in range(0, len(request_ids), 100):
                        batch_ids = request_ids[idx:idx + 100]
                        res = await mknetwork_client.check_status(batch_ids)
                        if isinstance(res, dict) and res.get("meta", {}).get("code") == 200:
                            results = res.get("data", {}).get("results", []) or []
                            for item in results:
                                req_id = item.get("request_id")
                                status = (item.get("status") or "").lower()
                                target_num = item.get("number") or f"+{item.get('full_number', '')}"
                                match = id_to_alloc.get(int(req_id)) if req_id is not None else None
                                if not match:
                                    continue
                                alloc_key, info = match

                                if status in ("expired", "not_found"):
                                    active_allocations.pop(alloc_key, None)
                                    continue

                                messages = item.get("messages") or []
                                if messages:
                                    for m in messages:
                                        sms_id = str(m.get("sms_id") or "")
                                        if sms_id:
                                            if len(seen_mk_sms_ids) > MAX_SEEN_SIZE:
                                                seen_mk_sms_ids.clear()
                                            if sms_id in seen_mk_sms_ids:
                                                continue
                                            seen_mk_sms_ids.add(sms_id)
                                        m_text = m.get("full_sms") or m.get("message") or ""
                                        m_otp = str(m.get("otp_code") or extract_code(m_text))
                                        m_sender = str(m.get("sender") or info.get("service") or "MKNETWORK")
                                        await dispatch_otp(app, info, target_num, m_otp, m_sender, m_text)
                                elif status in ("success", "multiple"):
                                    full_sms = item.get("full_sms") or ""
                                    otp_val = str(item.get("otp_code") or extract_code(full_sms))
                                    sender_val = str(item.get("sender") or info.get("service") or "MKNETWORK")
                                    dedupe_key = f"{req_id}_{otp_val}_{full_sms}"
                                    if dedupe_key not in seen_mk_sms_ids:
                                        if len(seen_mk_sms_ids) > MAX_SEEN_SIZE:
                                            seen_mk_sms_ids.clear()
                                        seen_mk_sms_ids.add(dedupe_key)
                                        await dispatch_otp(app, info, target_num, otp_val, sender_val, full_sms)
        except Exception as e:
            logging.error(f"Error in MKNetwork global poller: {e}")

        await asyncio.sleep(3.5)


async def global_voltxsms_poller(app):
    """Polls VoltxSMS /success-otp endpoint and dispatches OTPs matching active VoltxSMS allocations."""
    while True:
        try:
            if voltxsms_client and active_allocations:
                vx_entries = [
                    (alloc_key, info)
                    for alloc_key, info in list(active_allocations.items())
                    if info.get("provider") == "voltxsms"
                ]
                if vx_entries:
                    res = await voltxsms_client.get_success_otp()
                    if isinstance(res, dict) and res.get("meta", {}).get("code") == 200:
                        raw_data = res.get("data", {})
                        otps = raw_data.get("otps", []) if isinstance(raw_data, dict) else []
                        if isinstance(otps, dict):
                            otps = [otps]
                        for item in (otps or []):
                            if not isinstance(item, dict):
                                continue
                            otp_id = str(item.get("otp_id") or "")
                            raw_num = str(item.get("number") or item.get("full_number") or "")
                            msg_text = str(item.get("message") or "")
                            otp_code = extract_code(msg_text)

                            dedupe_key = otp_id or f"{raw_num}_{otp_code}_{msg_text}_{item.get('time', '')}"
                            if len(seen_voltx_otp_ids) > MAX_SEEN_SIZE:
                                seen_voltx_otp_ids.clear()
                            if dedupe_key in seen_voltx_otp_ids:
                                continue

                            norm_incoming = re.sub(r"\D", "", raw_num)
                            matched_any = False
                            for alloc_key, info in list(active_allocations.items()):
                                if info.get("provider") != "voltxsms":
                                    continue
                                alloc_num = str(info.get("number") or "")
                                norm_alloc = re.sub(r"\D", "", alloc_num)
                                if norm_alloc and norm_incoming and (
                                    norm_alloc == norm_incoming
                                    or norm_incoming.endswith(norm_alloc)
                                    or norm_alloc.endswith(norm_incoming)
                                ):
                                    target_num = alloc_num or (f"+{norm_incoming}" if norm_incoming else raw_num)
                                    sender_val = str(item.get("sender") or item.get("sid") or info.get("service") or "VoltxSMS")
                                    await dispatch_otp(app, info, target_num, otp_code, sender_val, msg_text)
                                    active_allocations.pop(alloc_key, None)
                                    matched_any = True

                            if matched_any:
                                seen_voltx_otp_ids.add(dedupe_key)
        except Exception as e:
            logging.error(f"Error in VoltxSMS global poller: {e}")

        await asyncio.sleep(3.0)


async def fake_sms_generator_loop(app):
    while True:
        try:
            await asyncio.sleep(fake_otp_settings["interval"])
            if fake_otp_settings["enabled"] and CHANNEL_CHAT_ID:
                all_ranges = await get_live_and_managed_ranges()
                if all_ranges:
                    chosen_range = random.choice(all_ranges)
                    app_name = chosen_range["service"]
                    country_name = chosen_range["country"]
                    flag_icon = chosen_range["flag"]
                    range_prefix = re.sub(r"\D", "", chosen_range["range"])[:5]

                    simulated_num = f"+{range_prefix}{''.join([str(random.randint(0, 9)) for _ in range(6)])}"
                    otp = str(random.randint(100000, 999999))
                    msg_text = f"<#> {otp} is your {app_name} verification code."
                    masked_num = mask_phone_number(simulated_num)

                    sender_label = (app_name or "OTP").strip()
                    country_label = (country_name or "Unknown").strip()
                    flag_label = (flag_icon or "🌐").strip()
                    code_label = str(otp or "").strip()
                    col1_w = 8
                    fake_table_block = "\n".join([
                        f"» {'NUMBER':<{col1_w}} | {masked_num}",
                        f"🌐 {'COUNTRY':<{col1_w}} | {flag_label} {country_label}",
                        f"❤ {'CODE':<{col1_w}} | {code_label}",
                    ])
                    fake_detail_line = format_otp_detail_line(sender_label, msg_text)
                    channel_text = (
                        f"📢 *{sender_label} • OTP • RCV*\n"
                        f"```text\n{fake_table_block}\n```\n"
                        f"✅ `{fake_detail_line}`"
                    )
                    channel_keyboard = await build_channel_keyboard(app.bot, otp, include_bot_button=True)
                    try:
                        await app.bot.send_message(chat_id=CHANNEL_CHAT_ID, text=channel_text, parse_mode="Markdown", reply_markup=channel_keyboard)
                    except Exception as err:
                        logging.error(f"Failed to post simulated SMS: {err}")

                    fake_dm_targets = []
                    if ADMIN_ID:
                        fake_dm_targets.append(ADMIN_ID)
                    for alloc_item in active_allocations.values():
                        if alloc_item.get("chat_id") and alloc_item["chat_id"] not in fake_dm_targets:
                            fake_dm_targets.append(alloc_item["chat_id"])
                    fake_dm_text = f"📱 `{simulated_num}`"
                    fake_dm_keyboard = InlineKeyboardMarkup([
                        [InlineKeyboardButton(f"{code_label}", copy_text=CopyTextButton(text=str(code_label)))]
                    ])
                    for dm_uid in fake_dm_targets:
                        try:
                            await app.bot.send_message(
                                chat_id=dm_uid,
                                text=fake_dm_text,
                                parse_mode="Markdown",
                                reply_markup=fake_dm_keyboard,
                            )
                        except Exception as dm_err:
                            logging.debug(f"Fake SMS DM skipped/failed for {dm_uid}: {dm_err}")

                    asyncio.create_task(
                        forward_otp_to_groups(
                            app=app,
                            target_number=simulated_num,
                            country_name=country_name,
                            flag_icon=flag_icon,
                            sender=sender_label,
                            otp_code=code_label,
                            msg_text=msg_text,
                        )
                    )
        except Exception as e:
            logging.error(f"Error in fake SMS loop: {e}")


def get_main_keyboard():
    keyboard = [
        [KeyboardButton("📱 Get Number 🟢"), KeyboardButton("⚡ Active Engine ⚡")],
        [KeyboardButton("🌐 Live Feed 🔵"), KeyboardButton("🎁 Referrals 🟡")],
        [KeyboardButton("🔐 2FA Online 🟣"), KeyboardButton("🎧 Support Hub 🔴")],
    ]
    return ReplyKeyboardMarkup(keyboard, resize_keyboard=True)


def _get_provider_label(prov: str) -> str:
    if prov == "mknetwork":
        return "MK Network"
    if prov == "voltxsms":
        return "VoltxSMS"
    return "ZebraSMS"


async def cmd_admin(update: Update, context: ContextTypes.DEFAULT_TYPE):
    context.user_data["waiting_for_range"] = None
    context.user_data["waiting_for_broadcast"] = False
    user_id = update.effective_user.id
    if ADMIN_ID != 0 and user_id != ADMIN_ID:
        await update.message.reply_text(f"⛔ **Access Denied:** Your Telegram ID `{user_id}` is not configured as admin.", parse_mode="Markdown")
        return

    keyboard = [
        [
            InlineKeyboardButton("🦓 ZebraSMS", callback_data="admin_prov_zebrasms"),
            InlineKeyboardButton("🌐 MK Network", callback_data="admin_prov_mknetwork"),
        ],
        [
            InlineKeyboardButton("⚡ VoltxSMS", callback_data="admin_prov_voltxsms"),
            InlineKeyboardButton("🧪 Toggle Fake SMS 🟠", callback_data="admin_toggle_fake"),
        ],
        [
            InlineKeyboardButton("📢 Broadcast Msg 🔵", callback_data="admin_broadcast"),
        ],
    ]

    fake_status = "🟢 ON" if fake_otp_settings["enabled"] else "🔴 OFF"
    admin_msg = (
        f"🛠 **Admin Configuration Panel** 🛠\n\n"
        f"🧪 **Fake SMS Simulation:** {fake_status} (Interval: {fake_otp_settings['interval']}s)\n\n"
        f"👇 *Select a provider to manage ranges or toggle settings:*"
    )
    await update.message.reply_text(admin_msg, parse_mode="Markdown", reply_markup=InlineKeyboardMarkup(keyboard))


async def admin_callback_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    user_id = query.from_user.id
    if ADMIN_ID != 0 and user_id != ADMIN_ID:
        await query.answer("Unauthorized action.", show_alert=True)
        return

    data = query.data

    if data.startswith("admin_prov_"):
        prov = data.replace("admin_prov_", "")
        prov_label = _get_provider_label(prov)
        context.user_data["admin_provider"] = prov
        managed_ranges = await asyncio.to_thread(db_get_managed_ranges, prov)

        ranges_text = "\n".join(
            [f"• {r['flag']} 🔹 **[{r['service']}]** {r['country']} (`{r['range']}`)" for r in managed_ranges]
        ) or "No active ranges configured."

        msg = (
            f"🛠 **Admin Panel: {prov_label}** 🛠\n\n"
            f"📋 **Current Active Ranges:**\n{ranges_text}\n\n"
            f"👇 *Manage ranges below:*"
        )
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
        context.user_data["waiting_for_range"] = None
        keyboard = [
            [
                InlineKeyboardButton("🦓 ZebraSMS", callback_data="admin_prov_zebrasms"),
                InlineKeyboardButton("🌐 MK Network", callback_data="admin_prov_mknetwork"),
            ],
            [
                InlineKeyboardButton("⚡ VoltxSMS", callback_data="admin_prov_voltxsms"),
                InlineKeyboardButton("🧪 Toggle Fake SMS 🟠", callback_data="admin_toggle_fake"),
            ],
            [
                InlineKeyboardButton("📢 Broadcast Msg 🔵", callback_data="admin_broadcast"),
            ],
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
        prov_label = _get_provider_label(prov)
        context.user_data["waiting_for_range"] = prov
        context.user_data["waiting_for_broadcast"] = False
        await query.message.reply_text(
            f"✍ **Send configuration for {prov_label}:**\n\n`Service | Country | Range`\n👉 *Example:* `WhatsApp | Ivory Coast | 22501XXX`",
            parse_mode="Markdown",
        )
        await query.answer()

    elif data.startswith("admin_del_select_"):
        prov = data.replace("admin_del_select_", "")
        await query.answer()
        managed_ranges = await asyncio.to_thread(db_get_managed_ranges, prov)
        if not managed_ranges:
            await query.edit_message_text("❌ No active ranges found to delete.")
            return

        keyboard = [
            [InlineKeyboardButton(f"❌ Delete {r['flag']} {r['service']} ({r['range']}) 🔴", callback_data=f"admin_del_{r['service']}_{r['range']}")]
            for r in managed_ranges
        ]
        keyboard.append([InlineKeyboardButton("🔙 Back", callback_data=f"admin_prov_{prov}")])
        await query.edit_message_text(f"🗑 **Select range to delete:**", reply_markup=InlineKeyboardMarkup(keyboard))

    elif data.startswith("admin_del_"):
        parts = data.replace("admin_del_", "").split("_", 1)
        if len(parts) == 2:
            srv_to_del, range_to_del = parts[0], parts[1]
            await asyncio.to_thread(db_delete_specific_range, range_to_del, srv_to_del)
            invalidate_liveaccess_cache()
            await query.answer(f"Deleted {srv_to_del} range {range_to_del}", show_alert=True)
            await query.edit_message_text(f"✅ **Range `{range_to_del}` for service `{srv_to_del}` deleted.**", parse_mode="Markdown")
        else:
            await query.answer("Invalid delete target.", show_alert=True)

    elif data == "admin_toggle_fake":
        fake_otp_settings["enabled"] = not fake_otp_settings["enabled"]
        status_text = "Enabled 🟢" if fake_otp_settings["enabled"] else "Disabled 🔴"
        await query.answer(f"Fake SMS Simulator is now {status_text}", show_alert=True)
        await query.edit_message_text(f"🧪 **Fake SMS Simulation Status:** {status_text}")

    elif data == "admin_broadcast":
        await query.answer()
        context.user_data["waiting_for_broadcast"] = True
        context.user_data["waiting_for_range"] = None
        await query.message.reply_text("📢 **Send the message you wish to broadcast:**", parse_mode="Markdown")

    elif data.startswith("admin_clear_"):
        prov = data.replace("admin_clear_", "")
        await asyncio.to_thread(db_clear_managed_ranges, prov)
        invalidate_liveaccess_cache()
        await query.answer("Ranges cleared!", show_alert=True)
        await query.edit_message_text("🗑 **All configured ranges cleared.**")


async def _allocate_single_provider_number(provider: str, range_val: str) -> tuple[str, str, str | None, int | None] | None:
    """Requests one number from ZebraSMS, MK Network v3, or VoltxSMS and normalizes the result."""
    if provider == "voltxsms" and voltxsms_client:
        res = await voltxsms_client.get_number(range_val=range_val)
        if isinstance(res, dict) and res.get("meta", {}).get("code") == 200:
            row = res.get("data")
            if isinstance(row, list) and row:
                row = row[0]
            if isinstance(row, dict):
                num_val = (
                    row.get("full_number")
                    or row.get("number")
                    or (f"+{row['no_plus_number']}" if row.get("no_plus_number") else None)
                )
                country_val = row.get("country")
                if num_val:
                    num_str = str(num_val).strip()
                    digits_only = re.sub(r"\D", "", num_str)
                    if not num_str.startswith("+") and digits_only.isdigit():
                        num_str = f"+{digits_only}"
                    alloc_key = f"vx_{digits_only or num_str}"
                    return alloc_key, num_str, country_val, None
        return None

    if provider == "mknetwork" and mknetwork_client:
        res = await mknetwork_client.get_number(range_val=range_val)
        if isinstance(res, dict) and res.get("meta", {}).get("code") == 200:
            row = res.get("data")
            if isinstance(row, list) and row:
                row = row[0]
            if isinstance(row, dict):
                num_val = row.get("number") or (f"+{row['full_number']}" if row.get("full_number") else None)
                req_id = row.get("request_id")
                country_val = row.get("country")
                if num_val:
                    alloc_key = f"mk_{req_id}" if req_id is not None else num_val
                    return alloc_key, num_val, country_val, int(req_id) if req_id is not None else None
        return None

    if zebrasms_client:
        res = await zebrasms_client.get_number(range_val=range_val)
        if isinstance(res, dict) and res.get("meta", {}).get("code") == 0:
            rows = res.get("data", {}).get("rows", []) or []
            if rows:
                row = rows[0]
                num_val = row.get("number")
                country_val = row.get("country")
                if num_val:
                    return num_val, num_val, country_val, None
    return None


async def user_provision_callback_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    data = query.data
    chat_id = query.message.chat_id

    if data.startswith("get_srv_"):
        await query.answer()
        selected_service = data.replace("get_srv_", "", 1)
        all_ranges = await get_live_and_managed_ranges()

        matching_ranges = [r for r in all_ranges if r["service"] == selected_service]

        if not matching_ranges:
            await query.edit_message_text(f"⚠ No ranges found for service `{selected_service}`.", parse_mode="Markdown")
            return

        country_buttons = [
            InlineKeyboardButton(
                f"{r['flag']} {r['country']} ({r['range']})",
                callback_data=f"prov_{r.get('provider', 'zebrasms')}_{r['range']}"
            )
            for r in matching_ranges
        ]
        keyboard = [country_buttons[i:i + 2] for i in range(0, len(country_buttons), 2)]
        keyboard.append([InlineKeyboardButton("🔙 Back to Services", callback_data="back_to_services")])

        await query.edit_message_text(
            f"🛠 *Selected Service:* `{selected_service}`\n\n👇 *Select Country / Active Range to allocate number:*",
            parse_mode="Markdown",
            reply_markup=InlineKeyboardMarkup(keyboard),
        )
        return

    elif data == "back_to_services":
        await query.answer()
        all_ranges = await get_live_and_managed_ranges()
        services = sorted(list(set(
            r["service"] for r in all_ranges if r.get("from_db") or is_allowed_social_sender(r["service"])
        )))
        if not services:
            await query.edit_message_text("⚠ No active social media ranges available right now.", parse_mode="Markdown")
            return

        keyboard = [[InlineKeyboardButton(f"🛡️ {srv} 🟢", callback_data=f"get_srv_{srv}")] for srv in services]
        await query.edit_message_text("📱 *Select a Service:*", parse_mode="Markdown", reply_markup=InlineKeyboardMarkup(keyboard))
        return

    elif data.startswith("prov_") or data.startswith("change_"):
        await query.answer()
        is_change = data.startswith("change_")
        clean_data = data.replace("change_", "", 1) if is_change else data.replace("prov_", "", 1)

        parts = clean_data.split("_", 1)
        chosen_provider = parts[0]
        selected_range = parts[1]

        if is_change:
            try:
                await query.message.delete()
            except Exception as e:
                logging.error(f"Error deleting old number message: {e}")

        all_ranges = await get_live_and_managed_ranges()
        matched_item = next(
            (r for r in all_ranges if r["range"] == selected_range and r.get("provider", "zebrasms") == chosen_provider),
            next((r for r in all_ranges if r["range"] == selected_range), {}),
        )

        c_name, flag_icon = auto_detect_country_and_flag(matched_item.get("country", ""), selected_range)

        loading_msg = await context.bot.send_message(
            chat_id=chat_id,
            text=f"⏳ Requesting 2 fresh numbers for {flag_icon} range `{selected_range}`...",
            parse_mode="Markdown"
        )

        allocated_results = []
        await asyncio.sleep(0.3)
        raw_allocs = await asyncio.gather(
            _allocate_single_provider_number(chosen_provider, selected_range),
            _allocate_single_provider_number(chosen_provider, selected_range),
        )
        seen_nums = set()
        for entry in raw_allocs:
            if entry:
                alloc_key, num_val, row_country, req_id = entry
                if num_val and num_val not in seen_nums:
                    seen_nums.add(num_val)
                    allocated_results.append((alloc_key, num_val, row_country, req_id))

        if allocated_results:
            first_entry = allocated_results[0]
            target_country = first_entry[2] if first_entry[2] else matched_item.get("country", c_name)
            final_country_name, final_flag = auto_detect_country_and_flag(target_country, first_entry[1])

            rand_fname, rand_lname = generate_random_english_name()
            number_copy_buttons = []

            for alloc_key, allocated_num, row_country, req_id in allocated_results:
                if alloc_key and allocated_num:
                    num_c_name, num_flag = auto_detect_country_and_flag(
                        row_country if row_country else final_country_name,
                        allocated_num,
                    )
                    active_allocations[alloc_key] = {
                        "chat_id": chat_id,
                        "country": num_c_name,
                        "flag": num_flag,
                        "number": allocated_num,
                        "provider": chosen_provider,
                        "service": matched_item.get("service", "OTP"),
                        "request_id": req_id,
                        "first_name": rand_fname,
                    }
                    number_copy_buttons.append(
                        [InlineKeyboardButton(f"📱 {allocated_num}", copy_text=CopyTextButton(text=str(allocated_num)))]
                    )

            msg = (
                f"✅ **Allocated ({len(allocated_results)}x)!**\n\n"
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
                f"❌ **Out of stock or API error hit.** Tap below to try again or choose another service.",
                parse_mode="Markdown",
                reply_markup=fallback_keyboard
            )

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

    elif data.startswith("copy_otp_"):
        otp_val = data.replace("copy_otp_", "")
        await query.answer(f"OTP Copied: {otp_val}", show_alert=True)


async def start_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    context.user_data["waiting_for_range"] = None
    context.user_data["waiting_for_broadcast"] = False
    context.user_data["waiting_for_2fa"] = False
    user_id = update.effective_user.id
    user_name = update.effective_user.first_name or "User"
    asyncio.create_task(asyncio.to_thread(db_add_user, user_id))

    welcome_msg = (
        f"👋 *ANU PREMIUM OTP BOT*\n\n"
        f"Welcome, *{user_name}*! 👋\n\n"
        f"Need help or want to add a working number? Contact support: {SUPPORT_USERNAME}"
    )
    await update.message.reply_text(welcome_msg, parse_mode="Markdown", reply_markup=get_main_keyboard())


async def handle_group_chat_tracking(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Automatically tracks group/supergroup chat IDs for country broadcasts."""
    chat = update.effective_chat
    if chat and chat.type in ("group", "supergroup"):
        asyncio.create_task(asyncio.to_thread(db_add_group, chat.id))


async def handle_text_messages(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat = update.effective_chat
    user_chat_id = chat.id
    if chat and chat.type in ("group", "supergroup"):
        asyncio.create_task(asyncio.to_thread(db_add_group, chat.id))
    else:
        asyncio.create_task(asyncio.to_thread(db_add_user, user_chat_id))

    if context.user_data.get("waiting_for_broadcast"):
        context.user_data["waiting_for_broadcast"] = False
        all_users = await asyncio.to_thread(db_get_all_users) or [user_chat_id]
        status_msg = await update.message.reply_text(f"⏳ Sending broadcast to {len(all_users)} users...")

        success, failed = 0, 0
        header = "📢 ANNOUNCEMENT 📢\n\n"

        for uid in all_users:
            try:
                if update.message.text:
                    orig_entities = list(update.message.entities or ())
                    shifted_entities = [
                        e for e in (
                            type(ent)(
                                offset=ent.offset + len(header),
                                length=ent.length,
                                type=ent.type,
                                url=ent.url,
                                user=ent.user,
                                language=ent.language,
                                custom_emoji_id=ent.custom_emoji_id,
                            )
                            for ent in orig_entities
                        )
                    ]
                    await context.bot.send_message(
                        chat_id=uid,
                        text=f"{header}{update.message.text}",
                        entities=shifted_entities,
                        reply_markup=update.message.reply_markup,
                    )
                else:
                    await update.message.copy(
                        chat_id=uid,
                        caption=f"{header}{update.message.caption}" if update.message.caption else None,
                        caption_entities=(
                            [
                                type(ent)(
                                    offset=ent.offset + len(header),
                                    length=ent.length,
                                    type=ent.type,
                                    url=ent.url,
                                    user=ent.user,
                                    language=ent.language,
                                    custom_emoji_id=ent.custom_emoji_id,
                                )
                                for ent in (update.message.caption_entities or ())
                            ]
                            if update.message.caption
                            else None
                        ),
                        reply_markup=update.message.reply_markup,
                    )
                success += 1
            except Exception as e:
                logging.error(f"Broadcast failed for user {uid}: {e}")
                failed += 1

        await status_msg.edit_text(
            f"✅ **Broadcast Completed!**\nSuccessful: `{success}`\n❌ Failed: `{failed}`",
            parse_mode="Markdown",
        )
        return

    text = update.message.text.strip() if update.message.text else ""

    menu_buttons = ("Get Number", "Active Engine", "Live Feed", "Referrals", "My Profile", "2FA Online", "Support Hub")
    if any(btn in text for btn in menu_buttons):
        context.user_data["waiting_for_range"] = None
        context.user_data["waiting_for_2fa"] = False

    if context.user_data.get("waiting_for_2fa"):
        context.user_data["waiting_for_2fa"] = False
        secret_key = text.strip().replace(" ", "")
        try:
            totp = pyotp.TOTP(secret_key)
            code = totp.now()
            remaining = 30 - (int(asyncio.get_event_loop().time()) % 30)
            refresh_keyboard = InlineKeyboardMarkup([[InlineKeyboardButton("🔄 Refresh Code", callback_data=f"refresh_2fa_{secret_key}")]])
            await update.message.reply_text(f"🔐 *Generated 2FA Code*\n\n🔑 Code: `{code}`\n⏳ Valid for ~{remaining}s", parse_mode="Markdown", reply_markup=refresh_keyboard)
        except Exception:
            await update.message.reply_text("❌ Invalid 2FA secret key!", reply_markup=get_main_keyboard())
        return

    if context.user_data.get("waiting_for_range"):
        prov = context.user_data.get("waiting_for_range")

        if text.count("|") != 2:
            await update.message.reply_text("⚠️ **Format Error!** Use: `Service | Country | Range`", parse_mode="Markdown")
            return

        parts = [p.strip() for p in text.split("|")]
        service = parts[0]
        country = parts[1]
        range_val = parts[2]

        c_name, detected_flag = auto_detect_country_and_flag(country, range_val)

        saved_ok = await asyncio.to_thread(db_add_managed_range, service, c_name, detected_flag, range_val, prov)
        context.user_data["waiting_for_range"] = None
        invalidate_liveaccess_cache()
        await get_live_and_managed_ranges(force_refresh=True)

        if saved_ok:
            await update.message.reply_text(
                f"✅ **Successfully saved range!**\n📌 Service: `{service}`\n{detected_flag} Country: `{c_name}`\n🔢 Range: `{range_val}`",
                parse_mode="Markdown",
                reply_markup=get_main_keyboard(),
            )
        else:
            await update.message.reply_text(
                "❌ **Failed to save range to database.** Check Supabase configuration or logs.",
                parse_mode="Markdown",
                reply_markup=get_main_keyboard(),
            )
        return

    if "Get Number" in text:
        all_ranges = await get_live_and_managed_ranges()
        services = sorted(list(set(
            r["service"] for r in all_ranges if r.get("from_db") or is_allowed_social_sender(r["service"])
        )))
        if not services:
            await update.message.reply_text("⚠ No active social media ranges available right now.", parse_mode="Markdown")
            return

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

    elif "2FA Online" in text:
        context.user_data["waiting_for_2fa"] = True
        await update.message.reply_text("🔐 **Send your 2FA Secret Key below:**", parse_mode="Markdown")

    elif "Support Hub" in text:
        await update.message.reply_text(f"🎧 **Support Hub**\n\nContact support agent directly {SUPPORT_USERNAME}", parse_mode="Markdown")


async def error_handler(update: object, context: ContextTypes.DEFAULT_TYPE) -> None:
    if isinstance(context.error, Exception) and "Conflict" in str(context.error):
        logging.warning("Telegram getUpdates conflict detected. Ensure only one bot instance is running.")
        return
    logging.error("Unhandled exception while handling update:", exc_info=context.error)


_cached_bot_username: str | None = None


async def get_cached_bot_username(bot) -> str:
    global _cached_bot_username
    if not _cached_bot_username:
        me = await bot.get_me()
        _cached_bot_username = (me.username or "").lstrip("@")
    return _cached_bot_username


async def _start_background_workers(application) -> None:
    await asyncio.sleep(1.0)
    await get_cached_bot_username(application.bot)
    application.create_task(dispatch_consumer_loop(application))
    application.create_task(global_zebrasms_poller(application))
    application.create_task(global_mknetwork_poller(application))
    application.create_task(global_voltxsms_poller(application))
    application.create_task(fake_sms_generator_loop(application))
    application.create_task(liveaccess_auto_refresh_loop())


async def post_init(application):
    asyncio.create_task(_start_background_workers(application))


# --- Main Execution ---

if __name__ == "__main__":
    keep_alive()
    print("Keep-alive HTTP server started.")

    app = ApplicationBuilder().token(TELEGRAM_BOT_TOKEN).post_init(post_init).build()

    app.add_error_handler(error_handler)

    app.add_handler(CommandHandler("start", start_command))
    app.add_handler(CommandHandler("admin", cmd_admin))
    app.add_handler(CommandHandler("test_sms", cmd_test_sms))

    app.add_handler(CallbackQueryHandler(admin_callback_handler, pattern="^admin_"))
    app.add_handler(CallbackQueryHandler(user_provision_callback_handler, pattern="^(get_srv_|back_to_services|prov_|change_|refresh_2fa_|copy_otp_)"))

    app.add_handler(MessageHandler(filters.ChatType.GROUPS & filters.StatusUpdate.ALL, handle_group_chat_tracking))
    app.add_handler(MessageHandler(filters.ALL & ~filters.COMMAND, handle_text_messages))

    print("🤖 Bot running with high-concurrency ZebraSMS + MK Network v3 + VoltxSMS integration...")
    app.run_polling(drop_pending_updates=True)
