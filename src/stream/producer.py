import time
import json
import random
import uuid
from kafka import KafkaProducer

# --- 1. KONFIGURASI ---
KAFKA_BOOTSTRAP_SERVERS = 'localhost:9092'
KAFKA_TOPIC_NAME = 'raw-faq-topic' # Kita ganti nama topic agar lebih sesuai

# --- 2. DATA SIMULASI (BAHASA INGGRIS) ---
# Disesuaikan dengan struktur dataset baru: 'question' dan 'answer'.
list_faq_examples = [
    {"question": "How do I check my order status?", "answer": "You can check your order status by visiting the 'My Orders' page in your account dashboard."},
    {"question": "What are the available payment methods?", "answer": "We accept credit cards, bank transfers, and various e-wallets."},
    {"question": "Can I cancel my order?", "answer": "Orders can be cancelled within 30 minutes of placement. After that, you may need to process a return."},
    {"question": "How long does shipping take?", "answer": "Standard shipping usually takes 3-5 business days. Express shipping takes 1-2 business days."},
    {"question": "What is the return policy?", "answer": "You can return any item within 14 days of receipt for a full refund."},
    {"question": "Do you ship internationally?", "answer": "Currently, we only ship within Indonesia."},
    {"question": "How can I contact customer support?", "answer": "You can contact us via the 'Contact Us' page or call our hotline at 1500-XXXX."},
]

# --- 3. INISIALISASI KAFKA PRODUCER ---
def create_kafka_producer(retries=5, delay=10):
    for i in range(retries):
        try:
            print(f"Attempting to connect to Kafka (Attempt {i+1}/{retries})...")
            producer = KafkaProducer(
                bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
                value_serializer=lambda v: json.dumps(v).encode('utf-8')
            )
            print("✅ Kafka Producer connected successfully!")
            return producer
        except Exception as e:
            print(f"❌ Failed to connect to Kafka: {e}")
            if i < retries - 1:
                print(f"Retrying in {delay} seconds...")
                time.sleep(delay)
            else:
                print("Could not connect to Kafka after several retries. Please ensure Kafka is running.")
                return None

# --- 4. FUNGSI UTAMA UNTUK MENGIRIM DATA ---
def send_data_stream():
    producer = create_kafka_producer()
    if not producer:
        return

    print(f"\n🚀 Starting to send simulated FAQ data to Kafka...")
    print(f"   Topic: {KAFKA_TOPIC_NAME}")
    print("   Press Ctrl+C to stop.\n")

    try:
        while True:
            # Pilih satu contoh FAQ secara acak
            faq_data = random.choice(list_faq_examples)
            
            # Buat pesan lengkap dengan metadata, sesuaikan nama field
            message = {
                'message_id': str(uuid.uuid4()),
                'timestamp': time.strftime('%Y-%m-%d %H:%M:%S'),
                'user_id': f"user_{random.randint(100, 999)}",
                'question': faq_data['question'],
                'answer_ground_truth': faq_data['answer'] # Jawaban asli dari FAQ
            }
            
            # Kirim pesan ke Kafka
            producer.send(KAFKA_TOPIC_NAME, value=message)
            
            # Tampilkan di terminal
            print(f"📤 Sending: {message}")
            
            # Tunggu beberapa detik sebelum mengirim pesan berikutnya
            time.sleep(random.uniform(2, 6)) # Jeda acak antara 2-6 detik

    except KeyboardInterrupt:
        print("\n Stream stopped by user.")
    finally:
        if producer:
            print("Closing Kafka Producer connection...")
            producer.flush()
            producer.close()
            print("Connection closed.")

# --- Jalankan program ---
if __name__ == "__main__":
    send_data_stream()