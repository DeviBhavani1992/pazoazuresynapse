import logging
import azure.functions as func
import json
import os
import requests
from datetime import datetime
import base64

from .adls_utils import (
    upload_json_to_adls,
    upload_image_to_adls,       # Kept for structural consistency
    upload_base64_to_adls,      # Kept for structural consistency
    save_image_and_base64
)

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
  "summary": "",
  "score": 8,
  "details": {
    "issues_found": [],
    "comments": "Any additional descriptive comments or context."
  }
}
"""

# ==============================
# CATEGORY PROMPTS — TOTAL 19 (ALL INCLUDED)
# ==============================
CATEGORY_PROMPTS = {

    # ===================
    # EXISTING 15 CATEGORIES
    # ===================

    "dresscode": """
Analyze employee dress code from the image:
- Shirt must be black or white.
- Pants must be black.
- Shoes must be present.
- Beard should not be present.
- Hair must not be frizzy and no weird hairstyles.
List all violations or missing required elements.
Provide a compliance rating from 1–10.
Return JSON only.
""",

    "dustbin": """
Analyze the dustbin in the image:
- Is the dustbin visible?
- Is it clean or untidy?
- Is a poly cover present?
- Is it overflowing?
List all missing or non-compliant items.
Provide a hygiene rating from 1–10.
Return JSON only.
""",

    "lightscheck": """
Analyze the lighting in the image:
- Identify lights that are ON.
- Identify lights that are OFF.
- Detect dim, flickering, or faulty lights.
Provide a lighting rating from 1–10.
Return JSON only.
""",

    "floorcheck": """
Analyze floor cleanliness:
- Check for hair, dust, stains, spills, or marks.
- Check whether the floor is dry and clean.
Provide a floor cleanliness rating from 1–10.
Return JSON only.
""",

    "nailpolishtray": """
Analyze the nail polish tray:
- Check if bottles are arranged neatly.
- Identify bottles with missing caps.
- Detect spills or stains.
Provide an organization rating from 1–10.
Return JSON only.
""",

    "shampoobottles": """
Analyze the shampoo bottle arrangement:
- Verify if bottles are arranged properly.
- Identify clutter or messy surroundings.
- Check for spills or stains.
Provide an arrangement rating from 1–10.
Return JSON only.
""",

    "restroomcheck": """
Analyze the restroom condition:
- Check if the toilet is clean.
- Check if the basin is clean.
- Identify stains or hair.
- Verify availability of handwash.
- Verify availability of room freshener.
Provide a restroom hygiene rating from 1–10.
Return JSON only.
""",

    "bedcheck": """
Analyze the bed setup:
- Check if a fresh disposable sheet is placed on the bed.
List issues such as missing or dirty sheet.
Provide a rating from 1–10.
Return JSON only.
""",

    "waxtinscheck": """
Analyze the wax tins:
- Verify if wax tins are covered with foil when not in use.
Provide a compliance rating from 1–10.
Return JSON only.
""",

    "pedicuresectioncheck": """
Analyze the pedicure section:
- Check if the floor is clean and free from stains.
- Check presence of required tools:
  - Cuticle pusher
  - Metal foot filer
  - Nail filer
  - Cuticle cutter
  - Nail cutter
Provide an overall rating from 1–10.
Return JSON only.
""",

    "eyebrowthreadkitcheck": """
Analyze the eyebrow threading kit:
- Verify presence of eyebrow thread.
- Verify presence of powder.
- Ensure both items are properly placed inside the kit.
Provide a completeness rating from 1–10.
Return JSON only.
""",

    "trolleycheck": """
Analyze the trolley products:
- Check for required products:
  - OSIS+ Dust It
  - OSIS+ Thrill
  - OSIS+ Flex Wax
Provide an availability rating from 1–10.
Return JSON only.
""",

    "sterilizercheck": """
Analyze the sterilizer:
- Check whether it is visible.
- Verify if it appears to be working (lights or indicators ON).
Provide a sterilizer condition rating from 1–10.
Return JSON only.
""",

    "hairwashstationcheck": """
Analyze the hair wash station:
- Check if the chair has a rubber neck protector.
- Check floor cleanliness for hair, stains, dirt.
- Evaluate overall station setup and hygiene.
Provide an overall rating from 1–10.
Return JSON only.
""",

    "facialroomstatuscheck": """
Analyze the facial room status:
- Identify if signage shows “In Progress” or “Ready for Service”.
- If signage is not visible, note it.
Provide a rating from 1–10.
Return JSON only.
""",


    # ===================
    # NEW 4 CATEGORIES
    # ===================

    "receptionareacheck": """
Analyze the reception area:
- Check if the reception desk is clean and organized.
- Check if the counter is free from hair and dust.
- Verify if printed bills are visible or being used.
- Check if marketing standees are placed and visible.
- Check if music is playing (if visually indicated).
- Verify if all reception-area lights are ON.
- Check if AC temperature indicator shows 20–22°C.
Provide a reception-area rating from 1–10.
Return JSON only.
""",

    "toolsterilizationcheck": """
Analyze the sterilization of tools:
- Check if clippers are sterilized.
- Check if trimmers are sterilized.
- Check if scissors are sterilized.
- Check if nail tools (cutters, filers, pushers) are sterilized.
Provide a tool sterilization rating from 1–10.
Return JSON only.
""",

    "haircutareacheck": """
Analyze the haircut area:
- Check the floor for hair, dust, stains, or spills.
- Verify if the workstation is clean and organized.
- Check if tools and products are neatly arranged.
- Ensure no hair is scattered around the chair or workstation.
Provide a haircut-area rating from 1–10.
Return JSON only.
""",

    "glassmirrorschairscheck": """
Analyze the glass, mirrors, and seating:
- Check if mirrors are clean and free from stains or smudges.
- Verify glass doors/partitions are clean and fingerprint-free.
- Confirm waiting chairs are clean and free from hair or dirt.
- Check if towels (if visible) are neatly arranged.
Provide a hygiene and presentation rating from 1–10.
Return JSON only.
"""
}

# ==============================
# MAIN FUNCTION
# ==============================
def main(req: func.HttpRequest) -> func.HttpResponse:
    logging.info("⚡ Azure Function triggered: Upload_image")

    try:
        # Validate ADLS environment variables (we use these two names)
        adls_conn = os.getenv("ADLS_CONNECTION_STRING")
        adls_container = os.getenv("ADLS_CONTAINER_NAME")

        # Trim accidental surrounding quotes (some appsettings had quotes when created)
        if adls_conn and (adls_conn.startswith('"') and adls_conn.endswith('"')):
            adls_conn = adls_conn.strip('"')

        if not adls_conn or not adls_container:
            return func.HttpResponse(
                json.dumps({"status": "error", "message": "Missing ADLS environment variables (ADLS_CONNECTION_STRING or ADLS_CONTAINER_NAME)"}),
                mimetype="application/json",
                status_code=500
            )

        # Read query params
        category = req.params.get("category")
        store_id = req.params.get("store_id")

        if not category or category not in CATEGORY_PROMPTS:
            return func.HttpResponse(
                json.dumps({"status": "error", "message": f"Invalid or missing category: {category}"}),
                mimetype="application/json",
                status_code=400
            )

        if not store_id:
            return func.HttpResponse(
                json.dumps({"status": "error", "message": "Missing ?store_id="}),
                mimetype="application/json",
                status_code=400
            )

        # Get uploaded file
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

        # Run AI inference
        ai_result = run_moondream_inference(prompt, file_content)
        logging.info(f"🤖 AI Response: {ai_result}")

        if "error" in ai_result:
            return func.HttpResponse(
                json.dumps({
                    "status": "error",
                    "filename": file.filename,
                    "message": ai_result.get("details", "Unknown error")
                }),
                mimetype="application/json",
                status_code=200
            )

        # --- FIX: Corrected function call for JSON upload (4 arguments) ---
        adls_path_json = upload_json_to_adls(ai_result, category, timestamp, store_id)

        # Upload raw image + base64 text to ADLS (helper)
        b64 = base64.b64encode(file_content).decode("utf-8")
        try:
            # --- FIX: Corrected function call for image/base64 upload (6 arguments) ---
            adls_paths_image = save_image_and_base64(
                file_content, 
                b64, 
                category, 
                store_id, 
                file.filename,
                timestamp
            )
        except Exception as e:
            # Don't fail the whole operation if image saving fails — log & continue
            logging.exception("Failed to save raw image/base64 to ADLS (non-fatal)")
            adls_paths_image = {"raw": "failed", "base64": "failed"}

        response_payload = {
            "status": "success",
            "filename": file.filename,
            "category": category,
            "adls_path_json": adls_path_json,
            "adls_path_raw_image": adls_paths_image.get("raw"),
            "adls_path_base64": adls_paths_image.get("base64"),
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
        full_prompt = f"{prompt}\n\n{JSON_RULE}"
        img_b64 = base64.b64encode(image_content).decode()

        payload = {
            "model": "moondream:latest",
            "prompt": full_prompt,
            "images": [img_b64],
            "stream": False
        }

        logging.info(f"📡 Sending request to OLLAMA → {OLLAMA_URL}")
        response = requests.post(OLLAMA_URL, json=payload, timeout=120)

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