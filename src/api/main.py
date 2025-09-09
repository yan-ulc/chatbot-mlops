from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
import mlflow
import pandas as pd

# --- 1. SETUP APLIKASI FASTAPI ---
app = FastAPI(
    title="FAQ Chatbot API",
    description="API untuk prediksi jawaban dari pertanyaan menggunakan model ML.",
    version="1.0.0"
)

# --- 2. DEFINISI MODEL DATA (Request & Response) ---
# Menentukan struktur JSON untuk input dari user
class QueryRequest(BaseModel):
    question: str

# Menentukan struktur JSON untuk output dari API
class PredictionResponse(BaseModel):
    predicted_answer: str

# --- 3. LOAD MODEL DARI MLFLOW ---
# Ganti dengan Run ID dari eksperimen terbaikmu di MLflow UI.
# Contoh: "runs:/<ID_RUN_ANDA>/model"
MLFLOW_RUN_ID = "ganti_dengan_run_id_anda" # PENTING: Ganti ini!
MODEL_URI = f"runs:/{MLFLOW_RUN_ID}/model"
loaded_model = None

@app.on_event("startup")
def load_model():
    """
    Fungsi ini akan berjalan saat aplikasi pertama kali dijalankan.
    Tugasnya adalah men-download dan memuat model dari MLflow.
    """
    global loaded_model
    try:
        print(f"Memuat model dari MLflow URI: {MODEL_URI}...")
        loaded_model = mlflow.sklearn.load_model(MODEL_URI)
        print("✅ Model berhasil dimuat!")
    except Exception as e:
        print(f"❌ Gagal memuat model: {e}")
        # Jika model gagal dimuat, aplikasi tidak bisa berjalan.
        # Di dunia nyata, ini bisa diganti dengan fallback model atau notifikasi.
        raise RuntimeError(f"Could not load model from MLflow: {e}")


# --- 4. BUAT ENDPOINT PREDIKSI ---
@app.post("/predict", response_model=PredictionResponse)
def predict(request: QueryRequest):
    """
    Endpoint utama untuk melakukan prediksi.
    Menerima pertanyaan dari user dan mengembalikan prediksi jawaban.
    """
    if loaded_model is None:
        raise HTTPException(status_code=503, detail="Model is not loaded yet. Please wait or check server logs.")

    print(f"Menerima request: {request.question}")
    
    # Buat DataFrame dari pertanyaan user, karena model kita dilatih dengan DataFrame
    question_df = pd.Series([request.question])
    
    # Lakukan prediksi menggunakan pipeline model yang sudah di-load
    prediction = loaded_model.predict(question_df)
    
    # Ambil hasil prediksi pertama (karena kita hanya memprediksi 1 data)
    predicted_answer = prediction[0]
    
    print(f"Hasil prediksi: {predicted_answer}")
    
    return PredictionResponse(predicted_answer=predicted_answer)

# --- 5. ENDPOINT UNTUK CEK STATUS ---
@app.get("/")
def read_root():
    return {"status": "FAQ Chatbot API is running"}