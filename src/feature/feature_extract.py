import pandas as pd
import numpy as np
from sentence_transformers import SentenceTransformer
import os
import joblib

# --- 1. SETUP ---
# Path input data yang sudah bersih
CLEANED_DATA_PATH = "data/processed/cleaned_faq_dataset.csv"
# Folder untuk menyimpan fitur yang sudah diekstrak
FEATURES_DIR = "data/featured"
# Path untuk file output
VECTORS_PATH = os.path.join(FEATURES_DIR, "question_vectors.npy")
ANSWERS_PATH = os.path.join(FEATURES_DIR, "answers.pkl")

# Nama model Sentence Transformer yang akan kita gunakan.
# 'all-MiniLM-L6-v2' adalah model yang sangat populer, cepat, dan bagus.
MODEL_NAME = 'all-MiniLM-L6-v2'

# --- 2. FUNGSI UTAMA ---
def build_features():
    """
    Memuat data bersih, mengekstrak vektor semantik dari pertanyaan,
    dan menyimpan hasilnya.
    """
    # Pastikan folder output sudah ada
    os.makedirs(FEATURES_DIR, exist_ok=True)

    # Muat dataset yang sudah dibersihkan
    print(f"📥 Membaca data bersih dari {CLEANED_DATA_PATH}...")
    try:
        df = pd.read_csv(CLEANED_DATA_PATH)
        df.dropna(subset=['question_clean', 'answer_original'], inplace=True)
    except FileNotFoundError:
        print(f"❌ Error: File tidak ditemukan di {CLEANED_DATA_PATH}.")
        print("Pastikan skrip preprocessor sudah dijalankan.")
        return
    
    if df.empty:
        print("❌ Error: Dataset kosong, tidak ada fitur untuk diekstrak.")
        return

    # Muat model Sentence Transformer
    print(f"🧠 Memuat model Sentence Transformer: '{MODEL_NAME}'...")
    # Model akan di-download otomatis saat pertama kali dijalankan
    model = SentenceTransformer(MODEL_NAME)
    print("✅ Model berhasil dimuat.")

    # Ekstrak fitur (encode teks menjadi vektor)
    questions_to_encode = df['question_clean'].tolist()
    print(f"🤖 Mengekstrak vektor dari {len(questions_to_encode)} pertanyaan...")
    
    # Proses encoding bisa memakan waktu beberapa saat tergantung jumlah data
    question_vectors = model.encode(questions_to_encode, show_progress_bar=True)

    # Simpan jawaban yang sesuai dengan urutan vektor
    answers = df['answer_original'].tolist()

    # Simpan hasil ekstraksi fitur
    print(f"💾 Menyimpan vektor ke {VECTORS_PATH}...")
    np.save(VECTORS_PATH, question_vectors)

    print(f"💾 Menyimpan jawaban ke {ANSWERS_PATH}...")
    joblib.dump(answers, ANSWERS_PATH)

    print("\n🎉 Ekstraksi fitur selesai!")
    print(f"   - Vektor pertanyaan disimpan di: {VECTORS_PATH}")
    print(f"   - Jawaban disimpan di: {ANSWERS_PATH}")
    print(f"   - Dimensi setiap vektor: {question_vectors.shape[1]}")


# --- 3. JALANKAN SKRIP ---
if __name__ == "__main__":
    build_features()