import pandas as pd
import re
import string
import os
import nltk
from nltk.corpus import stopwords

# --- 1. SETUP ---
# Pastikan stopwords sudah di-download
try:
    stopwords.words("english")
except LookupError:
    print("Downloading stopwords for English...")
    nltk.download('stopwords')

# Path untuk file input dan output
INPUT_PATH = "data/raw/ecommerce_faq_dataset.csv"
OUTPUT_DIR = "data/processed"
OUTPUT_PATH = os.path.join(OUTPUT_DIR, "cleaned_faq_dataset.csv")

# Definisikan stopwords sekali saja di luar fungsi agar lebih efisien
ENGLISH_STOP_WORDS = set(stopwords.words("english"))

# --- 2. FUNGSI CLEANING ---
def clean_text(text):
    """Fungsi untuk membersihkan teks: lowercase, hapus angka, tanda baca, whitespace, dan stopwords."""
    if pd.isna(text):
        return ""

    # Ubah ke huruf kecil (lowercase)
    text = text.lower()

    # Hapus angka
    text = re.sub(r"\d+", "", text)

    # Hapus tanda baca
    text = text.translate(str.maketrans("", "", string.punctuation))

    # Hapus spasi berlebih (extra whitespace)
    text = " ".join(text.split())

    # Hapus stopwords (kata-kata umum seperti 'the', 'a', 'in')
    tokens = [word for word in text.split() if word not in ENGLISH_STOP_WORDS]

    return " ".join(tokens)

# --- 3. FUNGSI UTAMA ---
def main():
    """Fungsi utama untuk memuat, memproses, dan menyimpan dataset."""
    # Pastikan folder output sudah ada
    os.makedirs(OUTPUT_DIR, exist_ok=True)

    # Muat dataset
    print(f"📥 Membaca dataset dari {INPUT_PATH}...")
    try:
        df = pd.read_csv(INPUT_PATH)
    except FileNotFoundError:
        print(f"❌ Error: File tidak ditemukan di {INPUT_PATH}.")
        print("Pastikan file 'ecommerce_faq_dataset.csv' sudah ada di folder 'data/raw/'.")
        return

    # Pastikan kolom yang dibutuhkan ada
    if not {"question", "answer"}.issubset(df.columns):
        raise ValueError("Dataset harus memiliki kolom 'question' dan 'answer'.")

    # Ambil hanya kolom yang kita butuhkan
    df_processed = df[['question', 'answer']].copy()

    # Bersihkan kolom 'question'
    print("🧹 Membersihkan kolom 'question'...")
    df_processed['question_clean'] = df_processed['question'].apply(clean_text)

    # Kolom 'answer' tidak perlu dibersihkan karena akan menjadi output akhir
    # Namun, jika Anda ingin membersihkannya juga, aktifkan baris di bawah ini:
    # print("🧹 Membersihkan kolom 'answer'...")
    # df_processed['answer_clean'] = df_processed['answer'].apply(clean_text)

    # Ganti nama kolom asli agar lebih jelas
    df_processed.rename(columns={'question': 'question_original', 'answer': 'answer_original'}, inplace=True)
    
    # Pilih dan urutkan kolom untuk file final
    final_columns = ['question_original', 'question_clean', 'answer_original']
    df_final = df_processed[final_columns]

    # Simpan hasil ke file CSV baru
    df_final.to_csv(OUTPUT_PATH, index=False)
    print(f"✅ Data berhasil diproses dan disimpan di {OUTPUT_PATH}")
    print("\nContoh 5 baris pertama data yang sudah dibersihkan:")
    print(df_final.head())


if __name__ == "__main__":
    main()