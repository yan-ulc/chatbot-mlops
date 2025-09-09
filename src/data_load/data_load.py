import pandas as pd
from datasets import load_dataset
import os

# --- 1. KONFIGURASI ---
# Nama dataset di Hugging Face Hub
DATASET_NAME = "NebulaByte/E-Commerce_FAQs"
# Lokasi folder untuk menyimpan data mentah
RAW_DATA_DIR = "data/raw"
# Nama file output yang kita inginkan
OUTPUT_FILENAME = "ecommerce_faq_dataset.csv"

# --- 2. FUNGSI UTAMA ---
def download_and_save_dataset():
    """
    Men-download dataset dari Hugging Face, mengubahnya menjadi DataFrame,
    dan menyimpannya sebagai file CSV.
    """
    try:
        print(f"Mencoba memuat dataset '{DATASET_NAME}' dari Hugging Face Hub...")
        # Load dataset, kita hanya butuh split 'train'
        dataset = load_dataset(DATASET_NAME, split="train")
        print("✅ Dataset berhasil dimuat!")

        # Ubah menjadi Pandas DataFrame
        df = pd.DataFrame(dataset)

        print("\nStruktur kolom dataset:")
        print(df.columns)
        
        print("\nContoh 5 baris pertama:")
        print(df.head())

        # Pastikan direktori tujuan sudah ada
        os.makedirs(RAW_DATA_DIR, exist_ok=True)
        
        # Tentukan path lengkap untuk file output
        output_path = os.path.join(RAW_DATA_DIR, OUTPUT_FILENAME)

        # Simpan DataFrame ke file CSV
        df.to_csv(output_path, index=False)
        
        print(f"\n🎉 Dataset berhasil disimpan di: {output_path}")

    except Exception as e:
        print(f"\n❌ Gagal memuat atau menyimpan dataset. Error: {e}")
        print("Pastikan nama dataset sudah benar dan komputermu terhubung ke internet.")

# --- 3. JALANKAN SKRIP ---
if __name__ == "__main__":
    download_and_save_dataset()