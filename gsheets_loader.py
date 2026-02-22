import pandas as pd
import requests
import urllib.parse
from google.oauth2 import service_account
from google.auth.transport.requests import Request
import streamlit as st

def get_service_account_creds():
    """Validates and returns credential object from st.secrets"""
    # Try to load from st.secrets first (handling nesting)
    try:
        # Check for our specific structure
        if "connections" in st.secrets and "gsheets" in st.secrets["connections"]:
            creds_info = st.secrets["connections"]["gsheets"]
        else:
            # Fallback for standard structure if changed
            creds_info = st.secrets
            
        # Helper to fix key if needed (though we fixed content in file)
        private_key = creds_info["private_key"]
        
        # Robustly fix newlines if they are escaped literals (just in case)
        if "\\n" in private_key:
            private_key = private_key.replace("\\n", "\n")
        
        # Mapping to standard service account dict
        service_account_info = {
            "type": creds_info.get("type", "service_account"),
            "project_id": creds_info["project_id"],
            "private_key_id": creds_info["private_key_id"],
            "private_key": private_key,
            "client_email": creds_info["client_email"],
            "client_id": creds_info["client_id"],
            "auth_uri": creds_info.get("auth_uri", "https://accounts.google.com/o/oauth2/auth"),
            "token_uri": creds_info.get("token_uri", "https://oauth2.googleapis.com/token"),
            "auth_provider_x509_cert_url": creds_info.get("auth_provider_x509_cert_url", "https://www.googleapis.com/oauth2/v1/certs"),
            "client_x509_cert_url": creds_info.get("client_x509_cert_url", "")
        }
        
        SCOPES = ['https://www.googleapis.com/auth/spreadsheets.readonly']
        
        creds = service_account.Credentials.from_service_account_info(
            service_account_info, scopes=SCOPES)
        return creds
        
    except Exception as e:
        st.error(f"Error creating credentials: {e}")
        return None

def load_google_sheet(sheet_url):
    """Loads a Google Sheet as a pandas DataFrame using direct API"""
    try:
        creds = get_service_account_creds()
        if not creds:
            return None
            
        if not creds.valid or creds.expired:
            creds.refresh(Request())
            
        # Parse ID from URL
        if "/d/" in sheet_url:
            spreadsheet_id = sheet_url.split("/d/")[1].split("/")[0]
        else:
            return None # Invalid URL

        # Check for GID (Worksheet ID) to target specific tab
        target_sheet_name = None
        gid = None
        if "gid=" in sheet_url:
            try:
                # Handle #gid=123 or ?gid=123
                if "#gid=" in sheet_url:
                    gid = sheet_url.split("#gid=")[1].split("&")[0]
                elif "?gid=" in sheet_url:
                    gid = sheet_url.split("?gid=")[1].split("&")[0]
                elif "&gid=" in sheet_url:
                    gid = sheet_url.split("&gid=")[1].split("&")[0]
            except:
                pass

        headers = {"Authorization": f"Bearer {creds.token}"}

        # If we have a GID, we need to find the sheet name
        if gid:
            meta_url = f"https://sheets.googleapis.com/v4/spreadsheets/{spreadsheet_id}"
            meta_resp = requests.get(meta_url, headers=headers)
            if meta_resp.status_code == 200:
                sheets = meta_resp.json().get('sheets', [])
                for s in sheets:
                    props = s.get('properties', {})
                    if str(props.get('sheetId')) == str(gid):
                        target_sheet_name = props.get('title')
                        break
        
        # Construct Range
        # If we found a name, use it. Otherwise default to 'A:ZZ' (first sheet)
        range_name = f"'{target_sheet_name}'!A:ZZ" if target_sheet_name else "A:ZZ"
        
        # Use simple Values API to get all data
        # URL Encode the range to handle slashes (e.g. '01/25') in sheet names
        # safe='' ensures slashes are encoded to %2F, which is required for path parameters
        encoded_range = urllib.parse.quote(range_name, safe='')
        api_url = f"https://sheets.googleapis.com/v4/spreadsheets/{spreadsheet_id}/values/{encoded_range}"
        
        resp = requests.get(api_url, headers=headers)
        
        if resp.status_code != 200:
            raise Exception(f"API Error {resp.status_code}: {resp.text}")
            
        data = resp.json()
        values = data.get('values', [])
        
        if not values:
            return pd.DataFrame()
            
        # Convert to DataFrame
        # Assume first row is header
        header = values[0]
        rows = values[1:]
        
        # Handle rows with varying lengths (API omits trailing empty cells)
        # Pad rows to match header length
        padded_rows = []
        for r in rows:
            if len(r) < len(header):
                r = r + [''] * (len(header) - len(r))
            padded_rows.append(r[:len(header)]) # Truncate if too long?
            
        df = pd.DataFrame(padded_rows, columns=header)
        return df
        
    except Exception as e:
        raise e

