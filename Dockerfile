FROM python:3.12-alpine

# Dependências de build
RUN apk add --no-cache --virtual .build-deps gcc musl-dev libffi-dev

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Limpar dependências de build
RUN apk del .build-deps

COPY . .

# Segurança: usuário não-root
RUN adduser -D -u 1000 appuser
USER appuser

ENV PORT=8080
EXPOSE 8080

CMD ["functions-framework", "--target=slack_webhook", "--port=8080"]
