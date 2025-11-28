#!/usr/bin/env python3
"""
Minimal PDF Download Proxy
Serves PDFs from Supabase Storage while logging all downloads
"""

from flask import Flask, render_template, request, send_file, abort, jsonify
from supabase import create_client, Client
from datetime import datetime, timezone
from io import BytesIO
import requests
import logging
import os
from urllib.parse import quote
from dotenv import load_dotenv
load_dotenv()

app = Flask(__name__)
logging.basicConfig(level=logging.INFO)
log = logging.getLogger("pdf-proxy")


SUPABASE_URL = os.environ.get("SUPABASE_URL")
SUPABASE_SERVICE_ROLE = os.environ.get("SUPABASE_SERVICE_ROLE")

if not SUPABASE_URL or not SUPABASE_SERVICE_ROLE:
    raise RuntimeError("Missing SUPABASE_URL or SUPABASE_SERVICE_ROLE")

sb: Client = create_client(SUPABASE_URL, SUPABASE_SERVICE_ROLE)


@app.get("/docs/<token>")
def docs_page(token: str):
    """Landing page with download button - logs the view"""
    
    result = sb.table("pdf_links").select("*").eq("token", token).execute()
    if not result.data:
        return "Invalid link.", 404
    
    info = result.data[0]
    
   
    expires = datetime.fromisoformat(info["expires_at"].replace("Z", "+00:00"))
    if datetime.now(timezone.utc) > expires:
        return "Link expired.", 410
    
    
    try:
        sb.table("pdf_views").insert({
            "token": token,
            "tracking_id": info.get("tracking_id"),
            "deal_id": info.get("deal_id"),
            "lender_name": info.get("lender_name"),
            "recipient_email": info.get("recipient_email"),
            "ip": request.headers.get("X-Forwarded-For", request.remote_addr),
            "user_agent": request.headers.get("User-Agent", "")[:500],
            "viewed_at": datetime.now(timezone.utc).isoformat(),
        }).execute()
        log.info(f"👁️ View: {info.get('lender_name')} - {info.get('recipient_email')}")
    except Exception as e:
        log.warning(f"Failed to log view: {e}")
    
    return render_template("docs.html", token=token, lender=info.get("lender_name", ""))


@app.get("/fetch/<token>")
def fetch_pdf(token: str):
    """Fetch PDF from Supabase, log download, serve to user"""
    try:
        
        result = sb.table("pdf_links").select("*").eq("token", token).execute()
        if not result.data:
            abort(404)
        
        info = result.data[0]
        
        
        expires = datetime.fromisoformat(info["expires_at"].replace("Z", "+00:00"))
        if datetime.now(timezone.utc) > expires:
            return "Link expired", 403
        
       
        storage_path = info["pdf_path"]
        public_url = f"{SUPABASE_URL}/storage/v1/object/public/secure-pdfs/{quote(storage_path)}"


        downloads = sb.table("pdf_downloads").select("*", count="exact").eq("token", token).execute()
        if downloads.count >= 2:
            return "Download limit exceeded.", 403
        
        
        pdf_response = requests.get(public_url)
        if pdf_response.status_code != 200:
            log.error(f"Failed to fetch PDF: {pdf_response.status_code}")
            abort(500)
        
        pdf_bytes = pdf_response.content
        
        
        sb.table("pdf_downloads").insert({
            "token": token,
            "tracking_id": info.get("tracking_id"),
            "deal_id": info.get("deal_id"),
            "lender_name": info.get("lender_name"),
            "recipient_email": info.get("recipient_email"),
            "ip": request.headers.get("X-Forwarded-For", request.remote_addr),
            "user_agent": request.headers.get("User-Agent", "")[:500],
            "downloaded_at": datetime.now(timezone.utc).isoformat(),
        }).execute()
        
        log.info(f"📥 Download: {info.get('lender_name')} - {info.get('recipient_email')}")
        
        
        filename = info.get("filename") or f"{info.get('lender_name', 'document')}.pdf"
        return send_file(
            BytesIO(pdf_bytes),
            mimetype="application/pdf",
            as_attachment=True,
            download_name=filename
        )
        
    except Exception as e:
        log.exception(f"fetch_pdf failed: {token}")
        abort(500)
        
if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5001, debug=True)
