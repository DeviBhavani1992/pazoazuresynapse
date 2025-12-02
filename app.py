import streamlit as st
import requests
from datetime import datetime
import logging

# Azure Function URL
AZURE_FUNCTION_URL = (
    "https://cavin-pazzo-20251015-ci.azurewebsites.net/api/Upload_image"
    "?code=F5MbFDI6XcXgRrbm7wX3JcyZdPzsOjswD2KCQROj9haWAzFuiNw41g=="
)

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

st.set_page_config(page_title="Pazo AI Portal", page_icon="✨")

st.title("✨  Image Analysis Dashboard")
st.markdown("Upload images for all AI Evaluation Categories below.")

# ASK USER FOR STORE ID
store_id = st.text_input("🏬 Enter Store ID", "SM-001")

# ============================
# UPDATED CATEGORIES — 19
# ============================
categories = {
    "dresscode": "👔 Dress Code Check",
    "dustbin": "🗑️ Dustbin Check",
    "lightscheck": "💡 Lights Check",
    "floorcheck": "🧹 Floor Check",
    "nailpolishtray": "💅 Nail Polish Tray Check",
    "shampoobottles": "🧴 Shampoo Bottles Check",
    "restroomcheck": "🚽 Rest Room Check",
    "bedcheck": "🛏️ Bed Setup Check",
    "waxtinscheck": "🕯️ Wax Tins Check",
    "pedicuresectioncheck": "💅 Pedicure Section Check",
    "eyebrowthreadkitcheck": "👁️ Eyebrow Thread Kit Check",
    "trolleycheck": "🛒 Trolley Products Check",
    "sterilizercheck": " Sterilizer Check",
    "hairwashstationcheck": "💇 Hair Wash Station Check",
    "facialroomstatuscheck": "💆 Facial Room Status Check",
    "receptionareacheck": "🏢 Reception Area Check",
    "toolsterilizationcheck": "✂️ Tool Sterilization Check",
    "haircutareacheck": "💈 Haircut Area Check",
    "glassmirrorschairscheck": "🪞 Glass, Mirrors & Chairs Check"
}

uploaded_files = {}

st.header("📸 Upload Images")

# Uploaders
for key, label in categories.items():
    with st.expander(label):
        uploaded_files[key] = st.file_uploader(
            f"Upload {label} Images",
            accept_multiple_files=True,
            type=["jpg", "jpeg", "png"],
            key=key
        )

if st.button("🚀 Submit All for AI Analysis"):
    results = []
    total_files = sum(len(files) for files in uploaded_files.values() if files)

    if total_files == 0:
        st.error("Please upload at least one image before submitting.")
        st.stop()

    st.info(f"Processing {total_files} images... Please wait ⏳")

    for category, files in uploaded_files.items():
        if not files:
            continue

        for file in files:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            fname = f"{category}_{timestamp}_{file.name}"

            files_payload = {
                "file": (fname, file.getvalue(), file.type)
            }

            # Pass store_id to Azure Function
            endpoint = (
                f"{AZURE_FUNCTION_URL}"
                f"&category={category}"
                f"&store_id={store_id}"
            )

            try:
                response = requests.post(endpoint, files=files_payload)

                if response.status_code == 200:
                    results.append(response.json())
                else:
                    results.append({
                        "filename": fname,
                        "category": category,
                        "status": "error",
                        "message": response.text
                    })

            except Exception as e:
                results.append({
                    "filename": fname,
                    "category": category,
                    "status": "error",
                    "message": str(e)
                })

    st.success("🎉 Analysis Completed!")

    st.header("📊 AI Results")
    for r in results:
        st.json(r)
