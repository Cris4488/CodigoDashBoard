import os, requests, io

def Get_Path_Documents():
    # usuario = getpass.getuser()
    ruta = os.path.expanduser('~')
    ruta = ruta + "\\Documents"
    return ruta

def Get_Token_Azure(tenant_id, client_id, client_secret): 
    token_url = f"https://login.microsoftonline.com/{tenant_id}/oauth2/v2.0/token"
    token_data = {
    "grant_type": "client_credentials",
    "client_id": client_id,
    "client_secret": client_secret,
    "scope": "https://graph.microsoft.com/.default"
    }
    token_r = requests.post(token_url, data=token_data)
    token_r.raise_for_status()
    access_token = token_r.json()["access_token"]
    headers = {"Authorization": f"Bearer {access_token}"}
    return headers

def Get_Site_Id(headers, site_name):
    site_url = f"https://graph.microsoft.com/v1.0/sites/sunshinebouquet1.sharepoint.com:/sites/{site_name}"
    site_r = requests.get(site_url, headers=headers)
    site_r.raise_for_status()
    site_info = site_r.json()
    site_id = site_info["id"]
    return site_id

def Get_Drive_Id(headers, site_id, main_folder):
    drives_url = f"https://graph.microsoft.com/v1.0/sites/{site_id}/drives"
    resp = requests.get(drives_url, headers=headers)
    resp.raise_for_status()
    for d in resp.json().get("value", []):
        if d['name'] == main_folder:
            drive_id = d['id']
    return drive_id

def Get_File_SH(headers, drive_id, folder, filename):
    files_url = f"https://graph.microsoft.com/v1.0/drives/{drive_id}/root:/{folder}:/children?$orderby=lastModifiedDateTime desc"
    files_r = requests.get(files_url, headers=headers)
    files_r.raise_for_status()
    files = files_r.json().get("value", [])
    target_file = next((f for f in files if f["name"] == filename), None)
    if target_file:
        download_url = target_file["@microsoft.graph.downloadUrl"]
        file_response = requests.get(download_url)
        file_response.raise_for_status()
        excel_bytes = io.BytesIO(file_response.content)
        return  excel_bytes
    else:
        print(f"Archivo {filename} no encontrado en {folder}")