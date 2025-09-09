import pickle

MODEL_PATH = "models/chatbot_model.pkl"

def load_model():
    with open(MODEL_PATH, "rb") as f:
        model = pickle.load(f)
    return model

model = load_model()

def predict_response(user_input: str) -> str:
    return model.predict([user_input])[0]
