import time
import json
import random
from kafka import KafkaProducer

def json_serializer(data):
    return json.dumps(data).encode('utf-8')

def main():
    print("==================================================")
    print("  INICIANDO PRODUCTOR DE STREAMING - NETFLIX")
    print("==================================================")

    # Conectar al broker de Kafka en localhost:9092
    producer = KafkaProducer(
        bootstrap_servers=['localhost:9092'],
        value_serializer=json_serializer
    )

    topic_name = 'netflix_streams'
    print(f"Transmitiendo eventos en vivo al tópico '{topic_name}'...
")

    # Muestra de datos simulados
    titles = [
        "Stranger Things", "The Crown", "Squid Game", "Money Heist",
        "Dark", "Black Mirror", "The Witcher", "Wednesday",
        "Ozark", "Mindhunter", "Narcos", "Bridgerton"
    ]
    
    actions = ["PLAY", "PAUSE", "STOP", "RATE_5_STAR", "BUFFERING"]
    countries = ["United States", "Colombia", "Mexico", "Spain", "United Kingdom", "Brazil"]

    user_id_counter = 1000

    try:
        while True:
            event = {
                "user_id": random.randint(1000, 1050),
                "title": random.choice(titles),
                "action": random.choice(actions),
                "country": random.choice(countries),
                "timestamp": int(time.time())
            }

            producer.send(topic_name, value=event)
            print(f"[ENVIADO] Evento: User {event['user_id']} | Action: {event['action']} | Title: '{event['title']}' | Country: {event['country']}")
            
            # Pausa de 1 segundo entre eventos
            time.sleep(1)

    except KeyboardInterrupt:
        print("
[INFO] Deteniendo el productor de Kafka...")
    finally:
        producer.close()
        print("[INFO] Productor cerrado correctamente.")

if __name__ == "__main__":
    main()
