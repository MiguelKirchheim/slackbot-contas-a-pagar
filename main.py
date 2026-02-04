import os
import re
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
SHEETS_RANGE = 'Sheet1!A:F'

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


def parse_date_to_month_folder(data_str):
    """
    Converte a data para formato de pasta mensal (YYYY-MM)
    Aceita formatos: DD/MM/YYYY, DD-MM-YYYY, YYYY-MM-DD
    """
    data_str = data_str.strip()

    # Tenta diferentes formatos de data
    formats = ['%d/%m/%Y', '%d-%m-%Y', '%Y-%m-%d', '%d/%m/%y', '%d-%m-%y']

    for fmt in formats:
        try:
            parsed_date = datetime.strptime(data_str, fmt)
            return parsed_date.strftime('%Y-%m')
        except ValueError:
            continue

    # Se não conseguir parsear, usa o mês atual
    return datetime.now().strftime('%Y-%m')


def get_or_create_month_folder(drive_service, parent_folder_id, month_name):
    """
    Busca ou cria a pasta do mês dentro da pasta pai.
    Retorna o ID da pasta do mês.
    """
    # Buscar pasta existente
    query = f"name='{month_name}' and '{parent_folder_id}' in parents and mimeType='application/vnd.google-apps.folder' and trashed=false"
    results = drive_service.files().list(
        q=query,
        spaces='drive',
        fields='files(id, name)'
    ).execute()

    files = results.get('files', [])

    if files:
        return files[0]['id']

    # Criar pasta se não existir
    folder_metadata = {
        'name': month_name,
        'mimeType': 'application/vnd.google-apps.folder',
        'parents': [parent_folder_id]
    }

    folder = drive_service.files().create(
        body=folder_metadata,
        fields='id'
    ).execute()

    return folder.get('id')


def sanitize_filename(text):
    """Remove caracteres inválidos para nome de arquivo"""
    # Remove caracteres especiais que não são permitidos em nomes de arquivo
    invalid_chars = ['/', '\\', ':', '*', '?', '"', '<', '>', '|']
    for char in invalid_chars:
        text = text.replace(char, '-')
    return text.strip()


def download_slack_file(file_info):
    """Baixa arquivo do Slack"""
    headers = {'Authorization': f'Bearer {SLACK_BOT_TOKEN}'}
    response = requests.get(
        file_info['url_private_download'],
        headers=headers
    )
    return response.content, file_info['name'], file_info['mimetype']


def upload_to_drive(drive_service, file_content, original_filename, mimetype, fields):
    """
    Faz upload do arquivo para o Google Drive.
    - Cria subpasta mensal baseada no campo DATA
    - Renomeia arquivo para: DATA - VALOR - BANCO - EMPRESA - CL.extensão
    """
    # Extrair extensão do arquivo original
    extension = ''
    if '.' in original_filename:
        extension = '.' + original_filename.rsplit('.', 1)[-1]

    # Criar nome do arquivo: DATA - VALOR - BANCO - EMPRESA - CL
    new_filename = f"{fields.get('DATA', '')} - {fields.get('VALOR', '')} - {fields.get('BANCO', '')} - {fields.get('EMPRESA', '')} - {fields.get('CL', '')}{extension}"
    new_filename = sanitize_filename(new_filename)

    # Determinar pasta do mês
    month_folder_name = parse_date_to_month_folder(fields.get('DATA', ''))
    month_folder_id = get_or_create_month_folder(drive_service, GOOGLE_DRIVE_FOLDER_ID, month_folder_name)

    file_metadata = {
        'name': new_filename,
        'parents': [month_folder_id]
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
        content, original_filename, mimetype = download_slack_file(file_info)
        link_comprovante = upload_to_drive(drive_service, content, original_filename, mimetype, fields)

    fields['LINK_COMPROVANTE'] = link_comprovante

    # Registrar no Sheets
    append_to_sheets(sheets_service, fields)

    return {'ok': True}
