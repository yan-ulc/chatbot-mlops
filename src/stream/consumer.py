import time
import json
import csv
import os
from kafka import KafkaConsumer

# --- 1. KONFIGURASI ---
# Pastikan KAFKA_TOPIC_NAME sama persis dengan yang ada di producer.
KAFKA_BOOTSTRAP_SERVERS = 'localhost:9092'
KAFKA_TOPIC_NAME = 'raw-faq-topic'
# ID grup untuk consumer, berguna jika kita punya banyak consumer.
KAFKA_GROUP_ID = 'faq-consumer-group-1'
# Lokasi file untuk menyimpan data yang diterima.
OUTPUT_CSV_PATH = 'data/streamed/streamed_faq_data.csv'

# --- 2. INISIALISASI KAFKA CONSUMER ---
def create_kafka_consumer():
    try:
        print("Mencoba koneksi ke Kafka sebagai Consumer...")
        consumer = KafkaConsumer(
            KAFKA_TOPIC_NAME,
            bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
            auto_offset_reset='earliest', # Mulai membaca dari pesan paling awal di topic
            group_id=KAFKA_GROUP_ID,
            # Mengubah data dari format JSON byte menjadi dictionary Python
            value_deserializer=lambda v: json.loads(v.decode('utf-8'))
        )
        print("✅ Koneksi Kafka Consumer berhasil!")
        return consumer
    except Exception as e:
        print(f"❌ Gagal koneksi ke Kafka: {e}")
        return None

# --- 3. FUNGSI UNTUK MENYIMPAN DATA KE CSV ---
def save_to_csv(data):
    # Cek apakah file sudah ada untuk menentukan perlu menulis header atau tidak
    file_exists = os.path.isfile(OUTPUT_CSV_PATH)
    
    with open(OUTPUT_CSV_PATH, mode='a', newline='', encoding='utf-8') as csv_file:
        # Tentukan nama kolom. Pastikan urutannya sesuai keinginan.
        fieldnames = ['message_id', 'timestamp', 'user_id', 'question', 'answer_ground_truth']
        writer = csv.DictWriter(csv_file, fieldnames=fieldnames)
        
        # Tulis header hanya jika file baru dibuat
        if not file_exists:
            writer.writeheader()
            
        # Tulis baris data
        writer.writerow(data)

# --- 4. FUNGSI UTAMA UNTUK MENERIMA DATA ---
def consume_data_stream():
    consumer = create_kafka_consumer()
    if not consumer:
        return

    print(f"\n🎧 Mulai mendengarkan data dari Kafka Topic: {KAFKA_TOPIC_NAME}")
    print(f"   Data akan disimpan di: {OUTPUT_CSV_PATH}")
    print("   Tekan Ctrl+C untuk berhenti.\n")

    try:
        # Loop ini akan berjalan selamanya, menunggu pesan baru masuk
        for message in consumer:
            # message.value berisi data dictionary yang dikirim producer
            data = message.value
            print(f"📥 Menerima: {data}")
            
            # Simpan data yang diterima ke dalam file CSV
            save_to_csv(data)
            
    except KeyboardInterrupt:
        print("\n🛑 Proses dihentikan oleh pengguna.")
    finally:
        if consumer:
            print("Menutup koneksi Kafka Consumer...")
            consumer.close()
            print("Koneksi ditutup.")

# --- Jalankan program ---
if __name__ == "__main__":
    # Pastikan direktori 'data/streamed/' sudah ada
    os.makedirs(os.path.dirname(OUTPUT_CSV_PATH), exist_ok=True)
    consume_data_stream()