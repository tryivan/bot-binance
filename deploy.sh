#!/bin/bash
# deploy.sh — Atualiza o bot-binance no Core 2 Duo

set -e
cd ~

PROJECT_DIR="$HOME/apps/bot-binance"
REPO_URL="https://github.com/tryivan/bot-binance.git"
ENV_FILE="$PROJECT_DIR/usdm_futures/shared/env/.env"
ENV_BACKUP="$HOME/.env_backup"
PYTHON_VERSION="3.11.12"

echo "=== Deploy bot-binance ==="

# 1. Para bots em execução
echo "Parando bots..."
tmux kill-server 2>/dev/null || true

# 2. Salva o .env
if [ -f "$ENV_FILE" ]; then
    echo "Salvando .env..."
    cp "$ENV_FILE" "$ENV_BACKUP"
else
    echo "AVISO: .env não encontrado."
fi

# 3. Salva os TOMLs dos pares
TOML_DIR="$PROJECT_DIR/usdm_futures/shared/data/symbol_configs"
TOML_BACKUP="$HOME/.toml_backup"
if [ -d "$TOML_DIR" ]; then
    echo "Salvando TOMLs..."
    rm -rf "$TOML_BACKUP"
    cp -r "$TOML_DIR" "$TOML_BACKUP"
fi

# 4. Remove o projeto
echo "Removendo projeto antigo..."
rm -rf "$PROJECT_DIR"

# 5. Clona o repositório
echo "Clonando repositório..."
git clone "$REPO_URL" "$PROJECT_DIR"
cd "$PROJECT_DIR"

# 6. Define Python 3.11
echo "Configurando Python $PYTHON_VERSION..."
pyenv local "$PYTHON_VERSION"

# 7. Cria virtualenv e instala dependências
echo "Criando virtualenv..."
python -m venv .venv
source .venv/bin/activate

echo "Criando requirements-legacy.txt..."
cat > requirements-legacy.txt << 'EOF'
numpy==1.26.4
pandas==2.0.3
ccxt
pydantic
pydantic-settings
aiohttp
scikit-learn
EOF

echo "Instalando dependências..."
pip install -q -r requirements-legacy.txt
pip install -q --no-deps pandas-ta-classic
pip install -q rich

# 8. Restaura o .env
if [ -f "$ENV_BACKUP" ]; then
    echo "Restaurando .env..."
    cp "$ENV_BACKUP" "$ENV_FILE"
fi

# 9. Restaura os TOMLs
if [ -d "$TOML_BACKUP" ]; then
    echo "Restaurando TOMLs..."
    mkdir -p "$TOML_DIR"
    cp "$TOML_BACKUP"/*.toml "$TOML_DIR/" 2>/dev/null || true
fi

echo ""
echo "=== Deploy concluído ==="
echo "Para iniciar: cd $PROJECT_DIR && source .venv/bin/activate && python launcher.py 3"