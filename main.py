import os
import re
import json
import requests
from datetime import datetime
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseUpload
import io
import functions_framework

# Configurações
SLACK_BOT_TOKEN = os.environ.get('SLACK_BOT_TOKEN')
SLACK_SIGNING_SECRET = os.environ.get('SLACK_SIGNING_SECRET')
GOOGLE_DRIVE_FOLDER_ID = os.environ.get('GOOGLE_DRIVE_FOLDER_ID')
GOOGLE_SHEETS_ID = os.environ.get('GOOGLE_SHEETS_ID')
SHEETS_RANGE = 'Sheet1!A:F'  # Ajuste conforme sua aba

# Regex para extrair campos da mensagem
FIELD_PATTERNS = {
    'DATA': r'DATA[:\s]+([^\n]+)',
    'VALOR': r'VALOR[:\s]+([^\n]+)',
    'BANCO': r'BANCO[:\s]+([^\n]+)',
    'EMPRESA': r'EMPRESA[:\s]+([^\n]+)',
    'CL': r'CL[:\s]+([^\n]+)',
}


def get_google_services():
    """Inicializa serviços do Google (Drive e Sheets)"""
    # Se estiver rodando no GCP, usa credenciais do ambiente
    # Se local, usar service account JSON
    creds = None
    if os.path.exists('service_account.json'):
        creds = service_account.Credentials.from_service_account_file(
            'service_account.json',
            scopes=[
                'https://www.googleapis.com/auth/drive',
                'https://www.googleapis.com/auth/spreadsheets'
            ]
        )

    drive_service = build('drive', 'v3', credentials=creds)
    sheets_service = build('sheets', 'v4', credentials=creds)

    return drive_service, sheets_service


def extract_fields(text):
    """Extrai os campos da mensagem usando regex"""
    fields = {}
    for field_name, pattern in FIELD_PATTERNS.items():
        match = re.search(pattern, text, re.IGNORECASE)
        fields[field_name] = match.group(1).strip() if match else ''
    return fields


def download_slack_file(file_info):
    """Baixa arquivo do Slack"""
    headers = {'Authorization': f'Bearer {SLACK_BOT_TOKEN}'}
    response = requests.get(
        file_info['url_private_download'],
        headers=headers
    )
    return response.content, file_info['name'], file_info['mimetype']


def upload_to_drive(drive_service, file_content, filename, mimetype):
    """Faz upload do arquivo para o Google Drive"""
    file_metadata = {
        'name': f"{datetime.now().strftime('%Y%m%d_%H%M%S')}_{filename}",
        'parents': [GOOGLE_DRIVE_FOLDER_ID]
    }

    media = MediaIoBaseUpload(
        io.BytesIO(file_content),
        mimetype=mimetype,
        resumable=True
    )

    file = drive_service.files().create(
        body=file_metadata,
        media_body=media,
        fields='id, webViewLink'
    ).execute()

    # Tornar o arquivo acessível via link
    drive_service.permissions().create(
        fileId=file['id'],
        body={'type': 'anyone', 'role': 'reader'}
    ).execute()

    return file.get('webViewLink')


def append_to_sheets(sheets_service, data):
    """Adiciona linha no Google Sheets"""
    values = [[
        data.get('DATA', ''),
        data.get('VALOR', ''),
        data.get('BANCO', ''),
        data.get('EMPRESA', ''),
        data.get('CL', ''),
        data.get('LINK_COMPROVANTE', '')
    ]]

    sheets_service.spreadsheets().values().append(
        spreadsheetId=GOOGLE_SHEETS_ID,
        range=SHEETS_RANGE,
        valueInputOption='USER_ENTERED',
        insertDataOption='INSERT_ROWS',
        body={'values': values}
    ).execute()


@functions_framework.http
def slack_webhook(request):
    """Endpoint principal que recebe eventos do Slack"""

    # Verificação de URL (Slack envia isso na configuração inicial)
    data = request.get_json()

    if data.get('type') == 'url_verification':
        return {'challenge': data['challenge']}

    # Ignorar eventos de retry ou do próprio bot
    if request.headers.get('X-Slack-Retry-Num'):
        return {'ok': True}

    event = data.get('event', {})

    # Processar apenas mensagens de usuários (não bots)
    if event.get('type') != 'message' or event.get('bot_id'):
        return {'ok': True}

    text = event.get('text', '')
    files = event.get('files', [])

    # Verificar se a mensagem tem os campos esperados
    fields = extract_fields(text)

    # Se não encontrou pelo menos DATA e VALOR, ignora
    if not fields.get('DATA') or not fields.get('VALOR'):
        return {'ok': True}

    # Inicializar serviços Google
    drive_service, sheets_service = get_google_services()

    # Se tiver arquivo, faz upload para o Drive
    link_comprovante = ''
    if files:
        file_info = files[0]  # Pega o primeiro arquivo
        content, filename, mimetype = download_slack_file(file_info)
        link_comprovante = upload_to_drive(drive_service, content, filename, mimetype)

    fields['LINK_COMPROVANTE'] = link_comprovante

    # Registrar no Sheets
    append_to_sheets(sheets_service, fields)

    return {'ok': True}
