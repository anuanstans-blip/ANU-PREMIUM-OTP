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

# Safely parse ADMIN_ID, removing any accidental quotes or spaces
_admin_raw = os.getenv("ADMIN_ID", "0").strip(" '\"")
ADMIN_ID = int(_admin_raw) if _admin_raw.isdigit() else 0

# Safely parse CHANNEL_CHAT_ID (handling negative chat IDs for channels/groups)
_channel_raw = os.getenv("CHANNEL_CHAT_ID", "0").strip(" '\"")
CHANNEL_CHAT_ID = int(_channel_raw) if _channel_raw.lstrip("-").isdigit() else 0

CHANNEL_URL = os.getenv("CHANNEL_URL", "https://t.me/").strip(" '\"")

ZEBRASMS_API_KEY = os.getenv("ZEBRASMS_API_KEY", "Q32FDUSCCGF").strip(" '\"")
ZEBRASMS_BASE_URL = os.getenv("ZEBRASMS_BASE_URL", "https://api.zebrasms.com/api/v1").strip(" '\"")

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

# High-throughput asynchronous dispatch queue
otp_dispatch_queue = asyncio.Queue()

# Track active background tasks per allocation key to prevent duplicate workers
active_worker_tasks = {}

# Fake OTP Generator State & Settings
fake_otp_settings = {
    "enabled": False,
    "interval": 15,
}

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


def db_get_managed_ranges(provider: str) -> list:
    if not supabase:
        return [r for r in DEFAULT_RANGES_ZEBRA if r.get("provider", "zebrasms") == provider]
    try:
        response = supabase.table("managed_ranges_zebra").select("*").eq("provider", provider).execute()
        if response.data:
            return response.data
        return []
    except Exception as e:
        logging.error(f"Error fetching ranges from Supabase: {e}")
        return []


def db_get_all_managed_ranges() -> list:
    """Retrieve all managed ranges for ZebraSMS."""
    if not supabase:
        return DEFAULT_RANGES_ZEBRA
    try:
        response = supabase.table("managed_ranges_zebra").select("*").execute()
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
        supabase.table("managed_ranges_zebra").upsert(data, on_conflict="service, range").execute()
    except Exception as e:
        logging.error(f"Error inserting range to Supabase: {e}")


def db_delete_specific_range(range_val: str, service: str = None):
    if not supabase:
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
        return
    try:
        supabase.table("managed_ranges_zebra").delete().eq("provider", provider).execute()
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
                logging.warning(f"[liveaccess] HTTP {response.status_code}: {response.text[:200]}")
                return {"meta": {"code": -500, "error": f"HTTP {response.status_code}"}}
            data = response.json()
            if not isinstance(data, dict) or data.get("meta", {}).get("code") != 0:
                logging.warning(f"[liveaccess] Non-zero meta: {data}")
            return data
        except Exception as e:
            logging.error(f"[liveaccess] Request error: {e}")
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


zebrasms_client = ZebraSMSClient(ZEBRASMS_API_KEY) if ZEBRASMS_API_KEY else None


async def get_live_and_managed_ranges() -> list:
    """
    Combines saved DB/fallback ranges with live /publicapi/liveaccess ranges:
    - Only includes active social media / messaging sender IDs.
    - Keeps exact sender casing (does not combine 'FACEBOOK' and 'Facebook').
    - Ignores duplicate ranges for the same sender ID.
    - Caps at 10 active ranges per sender ID (ignores 11th+).
    """
    per_sender_ranges = {}
    per_sender_seen = {}

    def add_range_entry(srv: str, rng: str, country: str, flag: str, provider: str, from_db: bool):
        srv_clean = (srv or "").strip()
        rng_clean = (rng or "").strip()
        if not srv_clean or not rng_clean:
            return
        if not is_allowed_social_sender(srv_clean):
            return

        per_sender_ranges.setdefault(srv_clean, [])
        per_sender_seen.setdefault(srv_clean, set())

        # Ignore duplicate range for the same sender ID
        if rng_clean in per_sender_seen[srv_clean]:
            return

        # Hard limit: max 10 active ranges per sender ID (ignore 11th+)
        if len(per_sender_ranges[srv_clean]) >= 10:
            return

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

    # 1. Saved DB / fallback ranges first
    for r in db_get_all_managed_ranges():
        add_range_entry(
            srv=r.get("service", ""),
            rng=r.get("range", ""),
            country=r.get("country", ""),
            flag=r.get("flag", ""),
            provider=r.get("provider", "zebrasms"),
            from_db=True,
        )

    # 2. Merge live delivering ranges from /publicapi/liveaccess
    if zebrasms_client:
        res = await zebrasms_client.get_live_access()
        if isinstance(res, dict) and res.get("meta", {}).get("code") == 0:
            rows = res.get("data", {}).get("rows", []) or []
            live_added = 0
            for row in rows:
                srv = (row.get("sender") or "").strip()
                if not srv or not is_allowed_social_sender(srv):
                    continue
                for rng in (row.get("ranges") or []):
                    if len(per_sender_ranges.get(srv, [])) >= 10:
                        break
                    before_len = len(per_sender_ranges.get(srv, []))
                    add_range_entry(
                        srv=srv,
                        rng=str(rng),
                        country="",
                        flag="",
                        provider="zebrasms",
                        from_db=False,
                    )
                    if len(per_sender_ranges.get(srv, [])) > before_len:
                        live_added += 1
            logging.info(f"[liveaccess] rows={len(rows)}, live_added={live_added}")

    combined = []
    for ranges_list in per_sender_ranges.values():
        combined.extend(ranges_list)
    return combined


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


async def cmd_test_sms(update: Update, context: ContextTypes.DEFAULT_TYPE):
    dummy_number = "+225011234567"
    dummy_sender = "WHATSAPP"
    dummy_message = "<#> 123456 is your verification code"
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

    if CHANNEL_CHAT_ID:
        channel_text = (
            "📢 *ⒶⓃⓊ ⓄⓉⓅ Ⓡⓔⓒⓔⓘⓥⓔⓓ*\n\n"
            f"📱 *To Number:* `{masked_num}`\n"
            f"{flag_icon} *Country:* {country_name}\n"
            f"👤 *Sender:* `{dummy_sender}`\n"
            f"💬 *Full Message:* `{dummy_message}`"
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
                channel_keyboard = await build_channel_keyboard(app.bot, otp_code, include_bot_button=True)
                try:
                    await app.bot.send_message(chat_id=CHANNEL_CHAT_ID, text=channel_text, parse_mode="Markdown", reply_markup=channel_keyboard)
                except Exception as err:
                    logging.error(f"Failed to post to channel: {err}")

            otp_dispatch_queue.task_done()
        except Exception as e:
            logging.error(f"Error in consumer loop: {e}")
            await asyncio.sleep(0.1)


async def dispatch_otp(app, allocation_info, target_number, otp_code, sender, msg_text):
    """Pushes OTP details into the non-blocking queue instantly."""
    await otp_dispatch_queue.put((allocation_info, target_number, otp_code, sender, msg_text))


# --- Global Checker Loop polling getupdate for all active allocations ---
async def global_zebrasms_poller(app):
    """Polls ZebraSMS /publicapi/getupdate endpoint and dispatches messages to matching active allocations."""
    while True:
        try:
            if zebrasms_client and active_allocations:
                res = await zebrasms_client.get_updates()
                if res.get("meta", {}).get("code") == 0:
                    rows = res.get("data", {}).get("rows", [])
                    for row in rows:
                        target_num = row.get("number") or ""
                        msg_text = row.get("message", "")
                        sender = row.get("sender", "UNKNOWN")
                        otp_code = extract_code(msg_text)

                        # Check if this phone number matches any currently active user allocation
                        for alloc_key, info in list(active_allocations.items()):
                            alloc_num = info.get("number") or ""
                            if alloc_num == target_num or alloc_num.replace("+", "") == target_num.replace("+", ""):
                                await dispatch_otp(app, info, target_num, otp_code, sender, msg_text)
                                # Remove allocation once code is captured/dispatched
                                active_allocations.pop(alloc_key, None)
        except Exception as e:
            logging.error(f"Error in ZebraSMS global poller: {e}")

        # Cached for 2 seconds per ZebraSMS docs, safe polling interval
        await asyncio.sleep(2.0)


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

                    channel_text = (
                        "📢 *ANU OTP Received*\n\n"
                        f"📱 *To Number:* `{masked_num}`\n"
                        f"{flag_icon} *Country:* {country_name}\n"
                        f"👤 *Sender:* `{app_name}`\n"
                        f"💬 *Full Message:* `{msg_text}`"
                    )
                    channel_keyboard = await build_channel_keyboard(app.bot, otp, include_bot_button=True)
                    try:
                        await app.bot.send_message(chat_id=CHANNEL_CHAT_ID, text=channel_text, parse_mode="Markdown", reply_markup=channel_keyboard)
                    except Exception as err:
                        logging.error(f"Failed to post simulated SMS: {err}")
        except Exception as e:
            logging.error(f"Error in fake SMS loop: {e}")


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
        [
            InlineKeyboardButton("🦓 ZebraSMS", callback_data="admin_prov_zebrasms"),
            InlineKeyboardButton("🧪 Toggle Fake SMS 🟠", callback_data="admin_toggle_fake"),
        ],
        [
            InlineKeyboardButton("📢 Broadcast Msg 🔵", callback_data="admin_broadcast"),
        ]
    ]

    fake_status = "🟢 ON" if fake_otp_settings["enabled"] else "🔴 OFF"
    admin_msg = (
        f"🛠 **Admin Configuration Panel** 🛠\n\n"
        f"🧪 **Fake SMS Simulation:** {fake_status} (Interval: {fake_otp_settings['interval']}s)\n\n"
        f"👇 *Select ZebraSMS to manage ranges or toggle settings:*"
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
                InlineKeyboardButton("🦓 ZebraSMS", callback_data="admin_prov_zebrasms"),
                InlineKeyboardButton("🧪 Toggle Fake SMS 🟠", callback_data="admin_toggle_fake"),
            ],
            [
                InlineKeyboardButton("📢 Broadcast Msg 🔵", callback_data="admin_broadcast"),
            ]
        ]
        fake_status = "🟢 ON" if fake_otp_settings["enabled"] else "🔴 OFF"
        admin_msg = (
            f"🛠 **Admin Configuration Panel** 🛠\n\n"
            f"🧪 **Fake SMS Simulation:** {fake_status} (Interval: {fake_otp_settings['interval']}s)\n\n"
            f"👇 *Select ZebraSMS to manage ranges or toggle settings:*"
        )
        await query.edit_message_text(admin_msg, parse_mode="Markdown", reply_markup=InlineKeyboardMarkup(keyboard))
        return

    elif data.startswith("admin_add_"):
        prov = data.replace("admin_add_", "")
        context.user_data["waiting_for_range"] = prov
        context.user_data["waiting_for_broadcast"] = False
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
        keyboard.append([InlineKeyboardButton("🔙 Back", callback_data=f"admin_prov_{prov}")])
        await query.edit_message_text(f"🗑 **Select range to delete:**", reply_markup=InlineKeyboardMarkup(keyboard))

    elif data.startswith("admin_del_"):
        parts = data.replace("admin_del_", "").split("_", 1)
        if len(parts) == 2:
            srv_to_del, range_to_del = parts[0], parts[1]
            db_delete_specific_range(range_to_del, service=srv_to_del)
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
        db_clear_managed_ranges(prov)
        await query.answer("All ZebraSMS ranges cleared!", show_alert=True)
        await query.edit_message_text("🗑 **All configured ZebraSMS ranges cleared.**")


async def user_provision_callback_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    data = query.data
    chat_id = query.message.chat_id

    if data.startswith("get_srv_"):
        await query.answer()
        selected_service = data.replace("get_srv_", "", 1)
        all_ranges = await get_live_and_managed_ranges()

        matching_ranges = [r for r in all_ranges if r["service"] == selected_service][:10]

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
            r["service"] for r in all_ranges if is_allowed_social_sender(r["service"])
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
            (r for r in all_ranges if r["range"] == selected_range),
            {}
        )

        c_name, flag_icon = auto_detect_country_and_flag(matched_item.get("country", ""), selected_range)

        loading_msg = await context.bot.send_message(
            chat_id=chat_id,
            text=f"⏳ Requesting 2 fresh numbers for {flag_icon} range `{selected_range}`...",
            parse_mode="Markdown"
        )

        allocated_results = []

        if zebrasms_client:
            await asyncio.sleep(0.3)
            responses = await asyncio.gather(
                zebrasms_client.get_number(range_val=selected_range),
                zebrasms_client.get_number(range_val=selected_range),
            )
            seen_nums = set()
            for res in responses:
                if isinstance(res, dict) and res.get("meta", {}).get("code") == 0:
                    rows = res.get("data", {}).get("rows", [])
                    if rows:
                        row = rows[0]
                        num_val = row.get("number")
                        if num_val and num_val not in seen_nums:
                            seen_nums.add(num_val)
                            allocated_results.append((num_val, num_val, row.get("country")))

        if allocated_results:
            first_entry = allocated_results[0]
            target_country = first_entry[2] if first_entry[2] else matched_item.get("country", c_name)
            final_country_name, final_flag = auto_detect_country_and_flag(target_country, first_entry[1])

            rand_fname, rand_lname = generate_random_english_name()
            number_copy_buttons = []

            for alloc_key, allocated_num, row_country in allocated_results:
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

    if context.user_data.get("waiting_for_broadcast"):
        context.user_data["waiting_for_broadcast"] = False
        all_users = db_get_all_users() or [user_chat_id]
        status_msg = await update.message.reply_text(f"⏳ Sending broadcast to {len(all_users)} users...")

        success, failed = 0, 0
        header = "📢 ANNOUNCEMENT 📢\n\n"

        for uid in all_users:
            try:
                if update.message.text:
                    # Preserve user's exact entities (bold, italic, mono, etc.) + prepend header
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
                    # Handles photo, video, document, audio, voice, animation, etc.
                    # along with caption_entities (bold, italic, mono in captions)
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

        db_add_managed_range(service, c_name, detected_flag, range_val, provider=prov)
        context.user_data["waiting_for_range"] = None

        await update.message.reply_text(
            f"✅ **Successfully saved range!**\n📌 Service: `{service}`\n{detected_flag} Country: `{c_name}`\n🔢 Range: `{range_val}`",
            parse_mode="Markdown",
            reply_markup=get_main_keyboard(),
        )
        return

    if "Get Number" in text:
        all_ranges = await get_live_and_managed_ranges()
        services = sorted(list(set(
            r["service"] for r in all_ranges if is_allowed_social_sender(r["service"])
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
    application.create_task(fake_sms_generator_loop(application))


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

    app.add_handler(MessageHandler(filters.ALL & ~filters.COMMAND, handle_text_messages))

    print("🤖 Bot running with high-concurrency ZebraSMS integration...")
    app.run_polling(drop_pending_updates=True)
