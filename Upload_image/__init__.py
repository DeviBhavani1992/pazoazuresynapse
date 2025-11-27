import logging
import azure.functions as func
import json
import os
import requests
from datetime import datetime
from .adls_utils import upload_json_to_adls

# ==============================
# OLLAMA ENDPOINT
# ==============================
OLLAMA_URL = os.getenv("OLLAMA_URL", "http://localhost:11434/api/generate")

# ==============================
# STRICT JSON RULE ENFORCEMENT
# ==============================
JSON_RULE = """
You MUST respond ONLY in valid JSON.
Do NOT include explanations, markdown, or any extra text.
Output must match this format exactly:
{
  "status": "pass or fail",
  "summary": "short summary",
  "score": "number or N/A",
  "details": {
    "issues_found": [],
    "comments": ""
  }
}
"""

# ==============================
# CATEGORY PROMPTS
# ==============================
CATEGORY_PROMPTS = {
    "dresscode": """
Analyze employee dress code from image:
- Shirt must be black or white
- Pants must be black
- Shoes must be present
- Beard should not be present
List violations and provide a rating.
Return JSON only.
""",
    "dustbin": """
Analyze dustbin:
- Is dustbin visible?
- Clean or untidy?
- Poly cover present?
- Overflowing or OK?
Return JSON only.
""",
    "lightscheck": """
Analyze lighting:
- Which lights are ON?
- Which lights are OFF?
- Any dim or faulty lights?
Return JSON only.
""",
    "floorcheck": """
Analyze floor cleanliness:
- Hair, dust, stains, spills, marks
- Is the floor dry and clean?
Provide a cleanliness rating.
Return JSON only.
""",
    "nailpolishtray": """
Analyze nail polish tray:
- Are bottles arranged neatly?
- Any bottles missing caps?
- Any spills or stains?
Return JSON only.
""",
    "shampoobottles": """
Analyze shampoo bottle arrangement:
- Are bottles arranged properly?
- Any messy surroundings?
- Any spills or stains?
Return JSON only.
""",
    "restroomcheck": """
Analyze restroom:
- Is toilet clean?
- Is basin clean?
- Any stains or hair?
- Handwash available?
- Room freshener available?
Provide rating.
Return JSON only.
"""
}

# ==============================
# MAIN FUNCTION
# ==============================
def main(req: func.HttpRequest) -> func.HttpResponse:
    logging.info("⚡ Azure Function triggered: Upload_image")

    try:
        # Validate ADLS environment variable
        if not os.getenv("ADLS_CONNECTION_STRING"):
            return func.HttpResponse(
                json.dumps({"status": "error", "message": "Missing ADLS_CONNECTION_STRING"}),
                mimetype="application/json",
                status_code=500
            )

        # Read query params
        category = req.params.get("category")
        store_id = req.params.get("store_id")

        if not category:
            return func.HttpResponse(
                json.dumps({"status": "error", "message": "Missing ?category="}),
                mimetype="application/json",
                status_code=400
            )

        if category not in CATEGORY_PROMPTS:
            return func.HttpResponse(
                json.dumps({"status": "error", "message": f"Invalid category: {category}"}),
                mimetype="application/json",
                status_code=400
            )

        if not store_id:
            return func.HttpResponse(
                json.dumps({"status": "error", "message": "Missing ?store_id="}),
                mimetype="application/json",
                status_code=400
            )

        # Get file
        file = req.files.get("file")
        if not file:
            return func.HttpResponse(
                json.dumps({"status": "error", "message": "Missing file upload"}),
                mimetype="application/json",
                status_code=400
            )

        logging.info(f"📁 Processing: {file.filename}, Category: {category}, Store: {store_id}")

        file_content = file.read()
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

        # Prepare prompt
        prompt = CATEGORY_PROMPTS[category]

        # Run model
        ai_result = run_moondream_inference(prompt, file_content)
        logging.info(f"🤖 AI Response: {ai_result}")

        if "error" in ai_result:
            return func.HttpResponse(
                json.dumps({
                    "status": "error",
                    "filename": file.filename,
                    "message": ai_result["details"]
                }),
                mimetype="application/json",
                status_code=200
            )

        # Upload JSON to ADLS
        adls_path = upload_json_to_adls(ai_result, category, timestamp, store_id)

        response_payload = {
            "status": "success",
            "filename": file.filename,
            "category": category,
            "adls_path": adls_path,
            "result": ai_result
        }

        return func.HttpResponse(
            json.dumps(response_payload),
            mimetype="application/json",
            status_code=200
        )

    except Exception as e:
        logging.exception("❌ Unhandled exception")
        return func.HttpResponse(
            json.dumps({"status": "error", "message": str(e)}),
            mimetype="application/json",
            status_code=500
        )

# ==============================
# MOONDREAM INFERENCE
# ==============================
def run_moondream_inference(prompt, image_content):
    try:
        import base64

        full_prompt = f"{prompt}\n\n{JSON_RULE}"

        img_b64 = base64.b64encode(image_content).decode()

        payload = {
            "model": "moondream:latest",
            "prompt": full_prompt,
            "images": [img_b64],
            "stream": False
        }

        logging.info(f"📡 Sending request to OLLAMA → {OLLAMA_URL}")
        response = requests.post(OLLAMA_URL, json=payload, timeout=60)

        if response.status_code != 200:
            return {"error": "ollama_error", "details": response.text}

        data = response.json()

        if "response" not in data:
            return {"error": "invalid_format", "details": data}

        # Parse JSON returned from model
        try:
            return json.loads(data["response"])
        except Exception:
            return {"error": "bad_json", "details": data["response"]}

    except Exception as e:
        logging.exception("🔥 Moondream Inference Error")
        return {"error": "exception", "details": str(e)}

