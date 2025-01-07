import psycopg2
from psycopg2 import sql
import random
from datetime import datetime, timedelta

conn = psycopg2.connect(
    dbname="pagila",
    user="postgres",
    password="123456",
    host="localhost",
    port="5432"
)
cur = conn.cursor()

countries = [
    "United States", "Canada", "Mexico", "Brazil", "Argentina", "United Kingdom", "Germany", "France", "Spain", "Italy",
    "Russia", "China", "Japan", "South Korea", "India", "Australia", "New Zealand", "South Africa", "Egypt", "Nigeria",
    "Kenya", "Israel", "Turkey", "Saudi Arabia", "Sweden", "Norway", "Denmark", "Finland", "Ireland", "Netherlands",
    "Switzerland", "Belgium", "Austria", "Portugal", "Greece", "Czech Republic", "Poland", "Hungary", "Romania", "Bulgaria",
    "Chile", "Peru", "Colombia", "Venezuela", "Uruguay", "Paraguay", "Ecuador", "Bolivia", "Iceland", "Luxembourg",
    "Monaco", "Malta", "Liechtenstein", "Singapore", "Malaysia", "Thailand", "Vietnam", "Indonesia", "Philippines", "Pakistan",
    "Bangladesh", "Sri Lanka", "Nepal", "Bhutan", "Maldives", "Afghanistan", "Iran", "Iraq", "Syria", "Jordan",
    "Lebanon", "Palestine", "Oman", "Qatar", "Kuwait", "Bahrain", "United Arab Emirates", "Yemen", "Uzbekistan", "Kazakhstan",
    "Azerbaijan", "Georgia", "Armenia", "Moldova", "Ukraine", "Belarus", "Lithuania", "Latvia", "Estonia", "Slovakia",
    "Slovenia", "Croatia", "Serbia", "Bosnia and Herzegovina", "Montenegro", "Albania", "North Macedonia", "Kosovo", "Mongolia", "Kyrgyzstan"
]

cities = {
    "United States": ["New York", "Los Angeles", "Chicago", "Houston", "Phoenix", "Philadelphia", "San Antonio", "San Diego", "Dallas", "San Jose"],
    "Canada": ["Toronto", "Vancouver", "Montreal", "Calgary", "Ottawa", "Edmonton", "Winnipeg", "Quebec City", "Hamilton", "Victoria"],
    "Mexico": ["Mexico City", "Guadalajara", "Monterrey", "Puebla", "Tijuana", "León", "Juárez", "Zapopan", "Cancún", "Culiacán"],
    "Brazil": ["São Paulo", "Rio de Janeiro", "Brasília", "Salvador", "Fortaleza", "Belo Horizonte", "Manaus", "Curitiba", "Recife", "Porto Alegre"],
    "Argentina": ["Buenos Aires", "Córdoba", "Rosario", "Mendoza", "La Plata", "Tucumán", "Mar del Plata", "Salta", "Santa Fe", "San Juan"],
    "United Kingdom": ["London", "Manchester", "Birmingham", "Leeds", "Glasgow", "Liverpool", "Edinburgh", "Bristol", "Newcastle", "Sheffield"],
    "Germany": ["Berlin", "Munich", "Frankfurt", "Hamburg", "Stuttgart", "Düsseldorf", "Cologne", "Leipzig", "Dresden", "Bremen"],
    "France": ["Paris", "Marseille", "Lyon", "Toulouse", "Nice", "Nantes", "Strasbourg", "Montpellier", "Bordeaux", "Lille"],
    "Italy": ["Rome", "Milan", "Naples", "Turin", "Palermo", "Genoa", "Bologna", "Florence", "Venice", "Verona"],
    "Spain": ["Madrid", "Barcelona", "Valencia", "Seville", "Zaragoza", "Malaga", "Murcia", "Palma", "Bilbao", "Alicante"],
    "Australia": ["Sydney", "Melbourne", "Brisbane", "Perth", "Adelaide", "Gold Coast", "Canberra", "Hobart", "Darwin", "Newcastle"],
    "Japan": ["Tokyo", "Osaka", "Yokohama", "Nagoya", "Sapporo", "Fukuoka", "Kobe", "Kyoto", "Sendai", "Hiroshima"],
    "China": ["Beijing", "Shanghai", "Guangzhou", "Shenzhen", "Chengdu", "Nanjing", "Wuhan", "Hangzhou", "Chongqing", "Xi'an"],
    "India": ["Mumbai", "Delhi", "Bangalore", "Hyderabad", "Ahmedabad", "Chennai", "Kolkata", "Pune", "Jaipur", "Surat"],
    "Russia": ["Moscow", "Saint Petersburg", "Novosibirsk", "Yekaterinburg", "Nizhny Novgorod", "Kazan", "Chelyabinsk", "Omsk", "Samara", "Rostov-on-Don"],
    "South Africa": ["Johannesburg", "Cape Town", "Durban", "Pretoria", "Port Elizabeth", "Bloemfontein", "East London", "Polokwane", "Pietermaritzburg", "Kimberley"],
    "Egypt": ["Cairo", "Alexandria", "Giza", "Shubra El-Kheima", "Port Said", "Suez", "Luxor", "Mansoura", "Tanta", "Asyut"],
    "Turkey": ["Istanbul", "Ankara", "Izmir", "Bursa", "Adana", "Gaziantep", "Konya", "Antalya", "Kayseri", "Mersin"],
    "South Korea": ["Seoul", "Busan", "Incheon", "Daegu", "Daejeon", "Gwangju", "Ulsan", "Suwon", "Goyang", "Yongin"],
    "Saudi Arabia": ["Riyadh", "Jeddah", "Mecca", "Medina", "Dammam", "Khobar", "Tabuk", "Abha", "Khamis Mushait", "Najran"],
    "Indonesia": ["Jakarta", "Surabaya", "Bandung", "Bekasi", "Medan", "Tangerang", "Depok", "Semarang", "Palembang", "Makassar"],
    "Nigeria": ["Lagos", "Abuja", "Kano", "Ibadan", "Port Harcourt", "Benin City", "Kaduna", "Enugu", "Zaria", "Jos"],
    "Philippines": ["Manila", "Quezon City", "Davao City", "Cebu City", "Zamboanga City", "Taguig", "Antipolo", "Pasig", "Cagayan de Oro", "Parañaque"]
}

streets = ["Main St", "Broadway", "1st Ave", "Maple St", "Elm St", "Park Ave", "2nd Ave", "Oak St", "Pine St", "Cedar St"]

def generate_address_data(city_id):
    street = random.choice(streets)
    street_number = random.randint(1, 1000)
    return f"{street_number} {street}", city_id

def generate_store_data(manager_staff_id, address_id):
    return manager_staff_id, address_id

def generate_staff_data():
    first_names = [
        "John", "Michael", "Robert", "James", "David", "William", "Richard", "Joseph", "Charles", "Thomas",
        "Christopher", "Daniel", "Matthew", "Anthony", "Mark", "Paul", "Steven", "Andrew", "Joshua", "Kevin",
        "Brian", "George", "Edward", "Ronald", "Timothy", "Jason", "Jeffrey", "Ryan", "Jacob", "Gary",
        "Nicholas", "Eric", "Jonathan", "Stephen", "Larry", "Justin", "Scott", "Brandon", "Benjamin", "Samuel",
        "Frank", "Gregory", "Raymond", "Alexander", "Patrick", "Jack", "Dennis", "Jerry", "Tyler", "Aaron",
        "Mary", "Jennifer", "Linda", "Elizabeth", "Susan", "Jessica", "Sarah", "Karen", "Nancy", "Lisa",
        "Margaret", "Betty", "Sandra", "Ashley", "Kimberly", "Emily", "Donna", "Michelle", "Carol", "Amanda",
        "Dorothy", "Melissa", "Deborah", "Stephanie", "Rebecca", "Laura", "Sharon", "Cynthia", "Kathleen", "Amy",
        "Shirley", "Angela", "Helen", "Anna", "Brenda", "Pamela", "Nicole", "Ruth", "Katherine", "Samantha",
        "Christine", "Emma", "Catherine", "Debra", "Virginia", "Rachel", "Carolyn", "Janet", "Maria", "Heather"

    ]
    last_names = [
        "Smith", "Johnson", "Williams", "Jones", "Brown", "Davis", "Miller", "Wilson", "Moore", "Taylor",
        "Anderson", "Thomas", "Jackson", "White", "Harris", "Martin", "Thompson", "Garcia", "Martinez", "Robinson",
        "Clark", "Rodriguez", "Lewis", "Lee", "Walker", "Hall", "Allen", "Young", "Hernandez", "King",
        "Wright", "Lopez", "Hill", "Scott", "Green", "Adams", "Baker", "Gonzalez", "Nelson", "Carter",
        "Mitchell", "Perez", "Roberts", "Turner", "Phillips", "Campbell", "Parker", "Evans", "Edwards", "Collins"
    ]
    first_name = random.choice(first_names)
    last_name = random.choice(last_names)
    email = f"{first_name.lower()}.{last_name.lower()}@example.com"
    address_id = random.randint(1, 1000)  # Поменяйте диапазон в зависимости от количества адресов
    store_id = random.randint(1, 2)  # Предположим, что в pagila два магазина
    active = True

    return first_name, last_name, address_id, store_id, email, active

def generate_rental_data():
    if random.random() < 0.9:
        customer_id = random.randint(1, 2)
    else:
        customer_id = random.randint(3, 599)

    staff_id = random.randint(1, 2002)
    inventory_id = random.randint(1, 4581)
    rental_date = datetime.now() - timedelta(days=random.randint(0, 30))

    return rental_date, inventory_id, customer_id, staff_id

try:
    #Заполнение стран и городов
    for country in countries:
        cur.execute("INSERT INTO country (country) VALUES (%s) RETURNING country_id", (country,))
        country_id = cur.fetchone()[0]

        # Заполнение таблицы city
        if country in cities:
            for city in cities[country]:
                cur.execute("INSERT INTO city (city, country_id) VALUES (%s, %s) RETURNING city_id", (city, country_id))
                city_id = cur.fetchone()[0]

                # Заполнение таблицы address
                for _ in range(100):  # 100 адресов на каждый город
                    street_address, city_id = generate_address_data(city_id)
                    cur.execute("INSERT INTO address (address, city_id) VALUES (%s, %s)", (street_address, city_id))

    # Заполнение таблицы store с привязкой к адресам
    for manager_staff_id in range(1, 11):  # Допустим, у нас 10 менеджеров для 10 магазинов
        address_id = random.randint(1, 100)  # Используем случайный address_id
        cur.execute("INSERT INTO store (manager_staff_id, address_id) VALUES (%s, %s)", (manager_staff_id, address_id))

    # Заполнение сотрудников staff
    for _ in range(2000):
        first_name, last_name, address_id, store_id, email, active = generate_staff_data()

        # Вставка новой записи
        cur.execute(
            sql.SQL("""
                INSERT INTO staff (first_name, last_name, address_id, store_id, email, active, username)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
            """),
            (first_name, last_name, address_id, store_id, email, active, f"{first_name.lower()}{random.randint(100, 999)}")
        )

    # Синхронизация последовательности
    cur.execute("SELECT setval('rental_rental_id_seq', (SELECT COALESCE(MAX(rental_id), 0) FROM rental) + 1);")

    # Заполнение таблицы rental
    for _ in range(10000000):
        rental_date, inventory_id, customer_id, staff_id = generate_rental_data()

        # Вставка с игнорированием дублирующихся записей по комбинации полей
        cur.execute(
            sql.SQL(
                """
                INSERT INTO rental (rental_date, inventory_id, customer_id, staff_id)
                VALUES (%s, %s, %s, %s)
                ON CONFLICT (rental_date, inventory_id, customer_id)
                DO NOTHING
                """
            ),
            (rental_date, inventory_id, customer_id, staff_id)
        )

    # Создание индексов для теста
    cur.execute("CREATE INDEX IF NOT EXISTS idx_rental_return_date ON rental (return_date);")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_rental_rental_date ON rental (rental_date);")
    cur.execute(
        "CREATE INDEX IF NOT EXISTS idx_rental_rental_date_rental_date_month ON rental (rental_date, EXTRACT(MONTH FROM rental_date));")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_film_upper_title ON film (UPPER(title));")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_film_upper_title_release_year ON film (UPPER(title), release_year);")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_rental_customer_id ON rental (customer_id);")

    # Сохранение изменений
    conn.commit()

except Exception as e:
    print("Произошла ошибка при добавлении сотрудников:", e)
    conn.rollback()

finally:
    cur.close()
    conn.close()
