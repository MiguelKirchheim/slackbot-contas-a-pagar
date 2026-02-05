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


def send_slack_message(channel, text, blocks=None):
    """Envia mensagem para um canal."""
    try:
        payload = {"channel": channel, "text": text}
        if blocks:
            payload["blocks"] = blocks
        response = requests.post(
            "https://slack.com/api/chat.postMessage",
            headers={"Authorization": f"Bearer {SLACK_BOT_TOKEN}"},
            json=payload,
        )
        result = response.json()
        if not result.get("ok"):
            logger.warning(f"Erro ao enviar mensagem: {result.get('error')}")
        return result
    except Exception as e:
        logger.warning(f"Falha ao enviar mensagem: {e}")
        return None


def open_slack_modal(trigger_id, view):
    """Abre um modal no Slack."""
    try:
        response = requests.post(
            "https://slack.com/api/views.open",
            headers={"Authorization": f"Bearer {SLACK_BOT_TOKEN}"},
            json={"trigger_id": trigger_id, "view": view},
        )
        result = response.json()
        if not result.get("ok"):
            logger.error(f"Erro ao abrir modal: {result.get('error')}")
        return result
    except Exception as e:
        logger.error(f"Falha ao abrir modal: {e}")
        return None


def get_file_info(file_id):
    """Obtem informacoes de um arquivo do Slack."""
    try:
        response = requests.get(
            "https://slack.com/api/files.info",
            headers={"Authorization": f"Bearer {SLACK_BOT_TOKEN}"},
            params={"file": file_id},
        )
        result = response.json()
        if result.get("ok"):
            return result.get("file", {})
        else:
            logger.warning(f"Erro ao obter info do arquivo: {result.get('error')}")
            return None
    except Exception as e:
        logger.warning(f"Falha ao obter info do arquivo: {e}")
        return None


# ============================================================
# MODAL - DEFINICAO
# ============================================================

def get_payment_modal():
    """Retorna a estrutura do modal de pagamento."""
    return {
        "type": "modal",
        "callback_id": "pagamento_modal",
        "title": {"type": "plain_text", "text": "Novo Pagamento"},
        "submit": {"type": "plain_text", "text": "Registrar"},
        "close": {"type": "plain_text", "text": "Cancelar"},
        "blocks": [
            {
                "type": "input",
                "block_id": "data_block",
                "label": {"type": "plain_text", "text": "Data do Pagamento"},
                "element": {
                    "type": "datepicker",
                    "action_id": "data_input",
                    "placeholder": {"type": "plain_text", "text": "Selecione a data"},
                },
            },
            {
                "type": "input",
                "block_id": "valor_block",
                "label": {"type": "plain_text", "text": "Valor"},
                "element": {
                    "type": "plain_text_input",
                    "action_id": "valor_input",
                    "placeholder": {"type": "plain_text", "text": "Ex: R$ 1.500,00"},
                },
            },
            {
                "type": "input",
                "block_id": "banco_block",
                "label": {"type": "plain_text", "text": "Banco"},
                "element": {
                    "type": "plain_text_input",
                    "action_id": "banco_input",
                    "placeholder": {"type": "plain_text", "text": "Ex: Itau, Bradesco"},
                },
            },
            {
                "type": "input",
                "block_id": "empresa_block",
                "label": {"type": "plain_text", "text": "Empresa"},
                "element": {
                    "type": "plain_text_input",
                    "action_id": "empresa_input",
                    "placeholder": {"type": "plain_text", "text": "Nome da empresa"},
                },
            },
            {
                "type": "input",
                "block_id": "cl_block",
                "label": {"type": "plain_text", "text": "CL (Centro de Lucro)"},
                "element": {
                    "type": "plain_text_input",
                    "action_id": "cl_input",
                    "placeholder": {"type": "plain_text", "text": "Ex: CC001"},
                },
                "optional": True,
            },
            {
                "type": "input",
                "block_id": "files_block",
                "label": {"type": "plain_text", "text": "Comprovantes"},
                "element": {
                    "type": "file_input",
                    "action_id": "files_input",
                    "filetypes": ["pdf", "jpg", "jpeg", "png"],
                    "max_files": 10,
                },
                "optional": True,
            },
        ],
    }


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
# HANDLERS - SLASH COMMANDS E INTERACTIONS
# ============================================================

def handle_slash_command(request):
    """Handler para slash commands (ex: /pagamento)."""
    # Slack envia como form-urlencoded
    command = request.form.get("command", "")
    trigger_id = request.form.get("trigger_id", "")
    user_id = request.form.get("user_id", "")
    channel_id = request.form.get("channel_id", "")

    logger.info(f"Slash command recebido: {command} de {user_id} no canal {channel_id}")

    if command == "/pagamento":
        # Abre o modal de pagamento
        modal = get_payment_modal()
        # Armazena o channel_id no private_metadata para usar depois
        modal["private_metadata"] = json.dumps({"channel_id": channel_id})
        result = open_slack_modal(trigger_id, modal)

        if result and result.get("ok"):
            return ""  # Resposta vazia = sucesso (Slack espera 200 vazio)
        else:
            return "Erro ao abrir formulario. Tente novamente."

    return ""


def handle_interaction(request):
    """Handler para interacoes (modal submissions, button clicks, etc)."""
    # Slack envia como form-urlencoded com campo 'payload' contendo JSON
    payload_str = request.form.get("payload", "{}")
    payload = json.loads(payload_str)

    interaction_type = payload.get("type", "")
    logger.info(f"Interaction recebida: {interaction_type}")

    if interaction_type == "view_submission":
        return handle_modal_submission(payload)

    return {"ok": True}


def handle_modal_submission(payload):
    """Processa a submissao do modal de pagamento."""
    callback_id = payload.get("view", {}).get("callback_id", "")

    if callback_id != "pagamento_modal":
        return {"ok": True}

    try:
        view = payload.get("view", {})
        user = payload.get("user", {})
        user_id = user.get("id", "")
        user_name = user.get("username", "")

        # Extrai dados do modal
        values = view.get("state", {}).get("values", {})

        # Data (datepicker retorna YYYY-MM-DD)
        data_iso = values.get("data_block", {}).get("data_input", {}).get("selected_date", "")
        # Converte para DD/MM/YYYY
        if data_iso:
            try:
                dt = datetime.strptime(data_iso, "%Y-%m-%d")
                data_br = dt.strftime("%d/%m/%Y")
            except ValueError:
                data_br = data_iso
        else:
            data_br = ""

        valor = values.get("valor_block", {}).get("valor_input", {}).get("value", "")
        banco = values.get("banco_block", {}).get("banco_input", {}).get("value", "")
        empresa = values.get("empresa_block", {}).get("empresa_input", {}).get("value", "")
        cl = values.get("cl_block", {}).get("cl_input", {}).get("value", "")

        # Arquivos (file_input)
        files_data = values.get("files_block", {}).get("files_input", {}).get("files", [])

        # Recupera o channel_id do private_metadata
        private_metadata = view.get("private_metadata", "{}")
        try:
            metadata = json.loads(private_metadata)
            channel_id = metadata.get("channel_id", "")
        except json.JSONDecodeError:
            channel_id = ""

        fields = {
            "DATA": data_br,
            "VALOR": valor,
            "BANCO": banco,
            "EMPRESA": empresa,
            "CL": cl,
        }

        logger.info(f"Modal submetido por {user_name}: {fields}")
        logger.info(f"Arquivos: {len(files_data)}")

        # Processa em background (responde rapidamente ao Slack)
        # O Slack espera resposta em 3 segundos
        process_modal_submission(fields, files_data, channel_id, user_id)

        # Fecha o modal com mensagem de sucesso
        return {
            "response_action": "clear",
        }

    except Exception as e:
        logger.error(f"Erro ao processar modal: {e}")
        traceback.print_exc()
        return {
            "response_action": "errors",
            "errors": {
                "valor_block": f"Erro ao processar: {str(e)}"
            }
        }


def process_modal_submission(fields, files_data, channel_id, user_id):
    """Processa os dados do modal (Drive + Sheets + notificacao)."""
    try:
        # Valida campos obrigatorios
        if not fields.get("DATA") or not fields.get("VALOR"):
            if channel_id:
                send_slack_message(channel_id, f"<@{user_id}> Erro: DATA e VALOR sao obrigatorios.")
            return

        # Inicializa servicos Google
        logger.info("Inicializando servicos Google...")
        drive, sheets = get_services()
        ensure_headers(sheets)

        # Cria pasta no Drive
        logger.info("Criando pasta no Drive...")
        folder_id, folder_link = create_lancamento_folder(drive, fields)
        logger.info(f"Pasta criada: {folder_link}")

        # Baixa e sobe arquivos
        file_count = 0
        if files_data:
            logger.info(f"Processando {len(files_data)} arquivo(s)...")
            for f in files_data:
                file_id = f.get("id")
                if not file_id:
                    continue

                # Obtem info do arquivo
                file_info = get_file_info(file_id)
                if not file_info:
                    continue

                # Baixa o arquivo
                url = file_info.get("url_private_download") or file_info.get("url_private")
                if not url:
                    continue

                headers = {"Authorization": f"Bearer {SLACK_BOT_TOKEN}"}
                response = requests.get(url, headers=headers)

                if response.status_code == 200:
                    filename = file_info.get("name", f"arquivo_{file_id}")
                    mimetype = file_info.get("mimetype", "application/octet-stream")
                    upload_file_to_drive(drive, folder_id, response.content, filename, mimetype)
                    file_count += 1
                else:
                    logger.warning(f"Erro ao baixar arquivo {file_id}: HTTP {response.status_code}")

        # Registra no Sheets
        logger.info("Salvando no Google Sheets...")
        append_to_sheets(sheets, fields, folder_link)
        logger.info("Registro salvo com sucesso!")

        # Notifica no canal
        if channel_id:
            blocks = [
                {
                    "type": "section",
                    "text": {
                        "type": "mrkdwn",
                        "text": f"*Pagamento registrado por <@{user_id}>*"
                    }
                },
                {
                    "type": "section",
                    "fields": [
                        {"type": "mrkdwn", "text": f"*Data:* {fields.get('DATA', '-')}"},
                        {"type": "mrkdwn", "text": f"*Valor:* {fields.get('VALOR', '-')}"},
                        {"type": "mrkdwn", "text": f"*Banco:* {fields.get('BANCO', '-')}"},
                        {"type": "mrkdwn", "text": f"*Empresa:* {fields.get('EMPRESA', '-')}"},
                        {"type": "mrkdwn", "text": f"*CL:* {fields.get('CL', '-')}"},
                        {"type": "mrkdwn", "text": f"*Arquivos:* {file_count}"},
                    ]
                },
                {
                    "type": "section",
                    "text": {
                        "type": "mrkdwn",
                        "text": f"<{folder_link}|Abrir pasta no Drive>"
                    }
                },
            ]

            send_slack_message(
                channel_id,
                f"Pagamento registrado por <@{user_id}>: {fields.get('EMPRESA', '')} - {fields.get('VALOR', '')}",
                blocks=blocks
            )

        logger.info(f"Pagamento processado com sucesso via modal: {fields}")

    except Exception as e:
        logger.error(f"Erro ao processar pagamento do modal: {e}")
        traceback.print_exc()
        if channel_id:
            send_slack_message(channel_id, f"<@{user_id}> Erro ao registrar pagamento: {str(e)}")


# ============================================================
# HANDLER PRINCIPAL
# ============================================================

@functions_framework.http
def slack_webhook(request):
    """
    Endpoint HTTP que recebe eventos do Slack.

    Rotas:
    - GET / : Health check
    - POST /slack/commands : Slash commands
    - POST /slack/interactions : Modal submissions e interacoes
    - POST / ou /slack/events : Eventos (mensagens)
    """

    # -- Health check GET --
    if request.method == 'GET':
        return {'status': 'ok', 'message': 'Slackbot Contas a Pagar esta rodando'}

    # -- Roteamento por path --
    path = request.path

    # Slash commands
    if path == "/slack/commands":
        if not verify_slack_signature(request):
            return ("Unauthorized", 403)
        return handle_slash_command(request)

    # Interactions (modal submissions)
    if path == "/slack/interactions":
        if not verify_slack_signature(request):
            return ("Unauthorized", 403)
        return handle_interaction(request)

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
