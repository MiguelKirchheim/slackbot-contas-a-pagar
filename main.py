"""
Slack Payment Bot - Cloud Function
===================================
Escuta mensagens no Slack, extrai campos de pagamento,
cria pasta no Drive com os anexos e registra no Sheets.

Formato esperado da mensagem:
    DATA: 04/02/2025
    VALOR: R$ 1.500,00
    BANCO: Itau
    EMPRESA: Empresa XYZ
    CL: CC001
    [arquivos anexados]

Estrutura de pastas no Drive:
    {PASTA_RAIZ}/
      {YYYY-MM}/
        {YYYY-MM-DD_VALOR_BANCO_EMPRESA_CL}/
          comprovante.pdf
          invoice.png
"""

import os
import re
import json
import hashlib
import hmac
import time
import logging
import traceback
import requests
import functions_framework
from datetime import datetime
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseUpload
import io

# ============================================================
# CONFIGURACAO
# ============================================================

SLACK_BOT_TOKEN = os.environ.get("SLACK_BOT_TOKEN")
SLACK_SIGNING_SECRET = os.environ.get("SLACK_SIGNING_SECRET")
GOOGLE_DRIVE_FOLDER_ID = os.environ.get("GOOGLE_DRIVE_FOLDER_ID")
GOOGLE_SHEETS_ID = os.environ.get("GOOGLE_SHEETS_ID")
SHEETS_TAB_NAME = os.environ.get("SHEETS_TAB_NAME", "Lancamentos")
SLACK_CHANNEL_ID = os.environ.get("SLACK_CHANNEL_ID", "")  # Opcional: filtrar canal

# Service Account credentials (JSON string ou path)
SA_CREDENTIALS_JSON = os.environ.get("SA_CREDENTIALS_JSON", "")
SA_CREDENTIALS_PATH = os.environ.get("SA_CREDENTIALS_PATH", "service_account.json")

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# ============================================================
# REGEX PARA EXTRACAO DOS CAMPOS
# ============================================================

FIELD_PATTERNS = {
    "DATA":    r"DATA\s*[:\-]\s*(.+?)(?:\n|$)",
    "VALOR":   r"VALOR\s*[:\-]\s*(.+?)(?:\n|$)",
    "BANCO":   r"BANCO\s*[:\-]\s*(.+?)(?:\n|$)",
    "EMPRESA": r"EMPRESA\s*[:\-]\s*(.+?)(?:\n|$)",
    "CL":      r"CL\s*[:\-]\s*(.+?)(?:\n|$)",
}

REQUIRED_FIELDS = ["DATA", "VALOR"]


# ============================================================
# GOOGLE SERVICES
# ============================================================

def get_google_credentials():
    """Obtem credenciais do Google a partir de variavel de ambiente ou arquivo."""
    scopes = [
        "https://www.googleapis.com/auth/drive",
        "https://www.googleapis.com/auth/spreadsheets",
    ]

    if SA_CREDENTIALS_JSON:
        info = json.loads(SA_CREDENTIALS_JSON)
        return service_account.Credentials.from_service_account_info(info, scopes=scopes)

    if os.path.exists(SA_CREDENTIALS_PATH):
        return service_account.Credentials.from_service_account_file(
            SA_CREDENTIALS_PATH, scopes=scopes
        )

    # No GCP, usa Application Default Credentials
    from google.auth import default
    creds, _ = default(scopes=scopes)
    return creds


def get_services():
    """Inicializa Drive e Sheets services."""
    creds = get_google_credentials()
    drive = build("drive", "v3", credentials=creds, cache_discovery=False)
    sheets = build("sheets", "v4", credentials=creds, cache_discovery=False)
    return drive, sheets


# ============================================================
# SLACK - VERIFICACAO E HELPERS
# ============================================================

def verify_slack_signature(request):
    """Verifica que a request realmente veio do Slack."""
    if not SLACK_SIGNING_SECRET:
        logger.warning("SLACK_SIGNING_SECRET nao configurado, pulando verificacao.")
        return True

    timestamp = request.headers.get("X-Slack-Request-Timestamp", "")
    signature = request.headers.get("X-Slack-Signature", "")

    if not timestamp or not signature:
        return True  # Headers nao presentes, permite (pode ser health check)

    try:
        if abs(time.time() - int(timestamp)) > 60 * 5:
            return False
    except ValueError:
        return False

    sig_basestring = f"v0:{timestamp}:{request.get_data(as_text=True)}"
    my_signature = "v0=" + hmac.new(
        SLACK_SIGNING_SECRET.encode(),
        sig_basestring.encode(),
        hashlib.sha256,
    ).hexdigest()

    return hmac.compare_digest(my_signature, signature)


def send_slack_reaction(channel, timestamp, emoji="white_check_mark"):
    """Adiciona uma reacao na mensagem do Slack como feedback."""
    try:
        response = requests.post(
            "https://slack.com/api/reactions.add",
            headers={"Authorization": f"Bearer {SLACK_BOT_TOKEN}"},
            json={"channel": channel, "timestamp": timestamp, "name": emoji},
        )
        result = response.json()
        if not result.get("ok"):
            logger.warning(f"Erro ao adicionar reacao: {result.get('error')}")
    except Exception as e:
        logger.warning(f"Falha ao enviar reacao: {e}")


def send_slack_reply(channel, thread_ts, text):
    """Responde na thread da mensagem."""
    try:
        response = requests.post(
            "https://slack.com/api/chat.postMessage",
            headers={"Authorization": f"Bearer {SLACK_BOT_TOKEN}"},
            json={"channel": channel, "thread_ts": thread_ts, "text": text},
        )
        result = response.json()
        if not result.get("ok"):
            logger.warning(f"Erro ao enviar reply: {result.get('error')}")
    except Exception as e:
        logger.warning(f"Falha ao enviar reply: {e}")


# ============================================================
# EXTRACAO DE CAMPOS
# ============================================================

def extract_fields(text: str) -> dict:
    """Extrai os campos da mensagem usando regex."""
    fields = {}
    for name, pattern in FIELD_PATTERNS.items():
        match = re.search(pattern, text, re.IGNORECASE)
        fields[name] = match.group(1).strip() if match else ""
    return fields


def has_required_fields(fields: dict) -> bool:
    """Verifica se os campos obrigatorios foram encontrados."""
    return all(fields.get(f) for f in REQUIRED_FIELDS)


# ============================================================
# GOOGLE DRIVE - PASTAS E UPLOAD
# ============================================================

def sanitize_folder_name(name: str) -> str:
    """Remove caracteres problematicos para nome de pasta."""
    return re.sub(r'[\\/:*?"<>|]', "_", name).strip()


def find_or_create_folder(drive, name: str, parent_id: str) -> str:
    """Busca pasta pelo nome dentro do parent. Cria se nao existir."""
    query = (
        f"name = '{name}' "
        f"and '{parent_id}' in parents "
        f"and mimeType = 'application/vnd.google-apps.folder' "
        f"and trashed = false"
    )
    # supportsAllDrives e includeItemsFromAllDrives para Shared Drives
    results = drive.files().list(
        q=query,
        fields="files(id)",
        supportsAllDrives=True,
        includeItemsFromAllDrives=True
    ).execute()
    files = results.get("files", [])

    if files:
        return files[0]["id"]

    metadata = {
        "name": name,
        "mimeType": "application/vnd.google-apps.folder",
        "parents": [parent_id],
    }
    folder = drive.files().create(
        body=metadata,
        fields="id",
        supportsAllDrives=True
    ).execute()
    logger.info(f"Pasta criada: {name} ({folder['id']})")
    return folder["id"]


def build_folder_name(fields: dict) -> str:
    """Monta o nome da pasta do lancamento a partir dos campos."""
    parts = []

    # Data no formato ISO para ordenacao
    data_raw = fields.get("DATA", "")
    try:
        dt = datetime.strptime(data_raw.strip(), "%d/%m/%Y")
        parts.append(dt.strftime("%Y-%m-%d"))
    except ValueError:
        parts.append(data_raw.replace("/", "-"))

    # Valor simplificado
    valor = fields.get("VALOR", "").replace(" ", "")
    parts.append(valor)

    # Banco e Empresa
    for field in ["BANCO", "EMPRESA", "CL"]:
        val = fields.get(field, "").strip()
        if val:
            parts.append(val)

    folder_name = "_".join(parts)
    return sanitize_folder_name(folder_name)


def get_month_folder_name(fields: dict) -> str:
    """Retorna o nome da pasta do mes (ex: 2025-02)."""
    data_raw = fields.get("DATA", "")
    try:
        dt = datetime.strptime(data_raw.strip(), "%d/%m/%Y")
        return dt.strftime("%Y-%m")
    except ValueError:
        # Fallback: mes atual
        return datetime.now().strftime("%Y-%m")


def create_lancamento_folder(drive, fields: dict) -> tuple:
    """
    Cria a estrutura de pastas:
        {ROOT}/{YYYY-MM}/{nome_lancamento}/

    Retorna (folder_id, web_view_link).
    """
    # 1. Pasta do mes
    month_name = get_month_folder_name(fields)
    month_folder_id = find_or_create_folder(drive, month_name, GOOGLE_DRIVE_FOLDER_ID)

    # 2. Pasta do lancamento
    lancamento_name = build_folder_name(fields)
    lancamento_folder_id = find_or_create_folder(drive, lancamento_name, month_folder_id)

    # Pega o link da pasta (supportsAllDrives para Shared Drives)
    folder_meta = (
        drive.files()
        .get(fileId=lancamento_folder_id, fields="webViewLink", supportsAllDrives=True)
        .execute()
    )

    return lancamento_folder_id, folder_meta.get("webViewLink", "")


def upload_file_to_drive(drive, folder_id: str, content: bytes, filename: str, mimetype: str):
    """Faz upload de um arquivo para a pasta no Drive."""
    metadata = {"name": filename, "parents": [folder_id]}
    media = MediaIoBaseUpload(io.BytesIO(content), mimetype=mimetype, resumable=True)

    # supportsAllDrives para Shared Drives
    uploaded = drive.files().create(
        body=metadata,
        media_body=media,
        fields="id, webViewLink",
        supportsAllDrives=True
    ).execute()

    logger.info(f"Arquivo uploaded: {filename} ({uploaded['id']})")
    return uploaded


# ============================================================
# SLACK - DOWNLOAD DE ARQUIVOS
# ============================================================

def download_slack_files(files: list) -> list:
    """Baixa todos os arquivos anexados na mensagem do Slack."""
    downloaded = []
    headers = {"Authorization": f"Bearer {SLACK_BOT_TOKEN}"}

    for f in files:
        url = f.get("url_private_download") or f.get("url_private")
        if not url:
            logger.warning(f"Arquivo sem URL de download: {f.get('name')}")
            continue

        response = requests.get(url, headers=headers)
        if response.status_code == 200:
            downloaded.append({
                "content": response.content,
                "name": f.get("name", "arquivo"),
                "mimetype": f.get("mimetype", "application/octet-stream"),
            })
        else:
            logger.error(f"Erro ao baixar {f.get('name')}: HTTP {response.status_code}")

    return downloaded


# ============================================================
# GOOGLE SHEETS - REGISTRO
# ============================================================

def append_to_sheets(sheets, fields: dict, folder_link: str):
    """Adiciona uma linha no Google Sheets com os dados do lancamento."""
    now = datetime.now().strftime("%d/%m/%Y %H:%M:%S")

    values = [[
        fields.get("DATA", ""),
        fields.get("VALOR", ""),
        fields.get("BANCO", ""),
        fields.get("EMPRESA", ""),
        fields.get("CL", ""),
        folder_link,
        now,  # Timestamp do registro
    ]]

    sheets.spreadsheets().values().append(
        spreadsheetId=GOOGLE_SHEETS_ID,
        range=f"{SHEETS_TAB_NAME}!A:G",
        valueInputOption="USER_ENTERED",
        insertDataOption="INSERT_ROWS",
        body={"values": values},
    ).execute()

    logger.info(f"Linha adicionada no Sheets: {fields.get('DATA')} - {fields.get('EMPRESA')}")


# ============================================================
# SETUP INICIAL DO SHEETS (headers)
# ============================================================

def ensure_headers(sheets):
    """Garante que a primeira linha tenha os cabecalhos."""
    try:
        result = (
            sheets.spreadsheets()
            .values()
            .get(spreadsheetId=GOOGLE_SHEETS_ID, range=f"{SHEETS_TAB_NAME}!A1:G1")
            .execute()
        )
        values = result.get("values", [])

        if not values:
            headers = [["DATA", "VALOR", "BANCO", "EMPRESA", "CL", "LINK PASTA", "REGISTRADO EM"]]
            sheets.spreadsheets().values().update(
                spreadsheetId=GOOGLE_SHEETS_ID,
                range=f"{SHEETS_TAB_NAME}!A1:G1",
                valueInputOption="RAW",
                body={"values": headers},
            ).execute()
            logger.info("Headers criados no Sheets.")
    except Exception as e:
        logger.warning(f"Erro ao verificar headers: {e}")


# ============================================================
# HANDLER PRINCIPAL
# ============================================================

@functions_framework.http
def slack_webhook(request):
    """
    Endpoint HTTP que recebe eventos do Slack.

    Fluxo:
    1. Verifica assinatura do Slack
    2. Responde url_verification (setup)
    3. Extrai campos da mensagem
    4. Cria pasta no Drive e sobe anexos
    5. Registra no Sheets
    6. Reage na mensagem do Slack como confirmacao
    """

    # -- Health check GET --
    if request.method == 'GET':
        return {'status': 'ok', 'message': 'Slackbot Contas a Pagar esta rodando'}

    # -- Verificacao de assinatura --
    if not verify_slack_signature(request):
        logger.warning("Assinatura invalida do Slack.")
        return ("Unauthorized", 403)

    data = request.get_json(force=True, silent=True)
    if not data:
        logger.warning("Requisicao sem JSON valido")
        return {"ok": True}

    logger.info(f"Payload recebido: {json.dumps(data, indent=2, default=str)}")

    # -- URL Verification (setup inicial do Slack) --
    if data.get("type") == "url_verification":
        return {"challenge": data["challenge"]}

    # -- Ignorar retries do Slack --
    if request.headers.get("X-Slack-Retry-Num"):
        return {"ok": True}

    event = data.get("event", {})

    # Processar apenas mensagens de usuarios
    if event.get("type") != "message":
        return {"ok": True}

    # Ignorar mensagens de bots
    if event.get("bot_id"):
        return {"ok": True}

    # Ignorar subtypes que nao sao mensagens normais (exceto file_share que tem anexos)
    subtype = event.get("subtype")
    if subtype and subtype not in ["file_share"]:
        return {"ok": True}

    # Filtrar por canal especifico (se configurado)
    channel = event.get("channel", "")
    if SLACK_CHANNEL_ID and channel != SLACK_CHANNEL_ID:
        return {"ok": True}

    text = event.get("text", "")
    files = event.get("files", [])
    message_ts = event.get("ts", "")

    # -- Extrair campos --
    fields = extract_fields(text)
    logger.info(f"Campos extraidos: {fields}")

    if not has_required_fields(fields):
        return {"ok": True}  # Mensagem nao e um lancamento, ignora silenciosamente

    try:
        # -- Inicializar servicos Google --
        logger.info("Inicializando servicos Google...")
        logger.info(f"DRIVE_FOLDER_ID: {GOOGLE_DRIVE_FOLDER_ID}")
        logger.info(f"SHEETS_ID: {GOOGLE_SHEETS_ID}")
        logger.info(f"SHEETS_TAB_NAME: {SHEETS_TAB_NAME}")

        drive, sheets = get_services()
        ensure_headers(sheets)

        # -- Criar pasta no Drive --
        logger.info("Criando pasta no Drive...")
        folder_id, folder_link = create_lancamento_folder(drive, fields)
        logger.info(f"Pasta criada: {folder_link}")

        # -- Baixar e subir arquivos --
        file_count = 0
        if files:
            logger.info(f"Baixando {len(files)} arquivo(s) do Slack...")
            downloaded = download_slack_files(files)
            for f in downloaded:
                upload_file_to_drive(drive, folder_id, f["content"], f["name"], f["mimetype"])
            file_count = len(downloaded)

        # -- Registrar no Sheets --
        logger.info("Salvando no Google Sheets...")
        append_to_sheets(sheets, fields, folder_link)
        logger.info("Registro salvo com sucesso!")

        # -- Feedback no Slack --
        send_slack_reaction(channel, message_ts, "white_check_mark")

        if file_count > 0:
            reply = (
                f"*Lancamento registrado!*\n"
                f"Pasta: {folder_link}\n"
                f"{file_count} arquivo(s) salvos"
            )
        else:
            reply = (
                f"*Lancamento registrado!*\n"
                f"Pasta: {folder_link}\n"
                f"Nenhum comprovante anexado"
            )
        send_slack_reply(channel, message_ts, reply)

        logger.info(f"Lancamento processado com sucesso: {fields}")

    except Exception as e:
        logger.error(f"Erro ao processar lancamento: {e}")
        traceback.print_exc()
        send_slack_reaction(channel, message_ts, "x")
        send_slack_reply(channel, message_ts, f"Erro ao processar lancamento: {str(e)}")

    return {"ok": True}
