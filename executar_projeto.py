import os
import sys
import time
import socket
import signal
import threading
import subprocess
from pathlib import Path

# Diretório raiz do projeto e arquivo de log
BASE_DIR = Path(__file__).resolve().parent
LOG_FILE = BASE_DIR / "saida.txt"


class TeeLogger:
    """Classe responsável por duplicar a saída do terminal para um arquivo de texto."""
    def __init__(self, filepath):
        self.file = open(filepath, "w", encoding="utf-8", buffering=1)
        self.terminal_stdout = sys.stdout
        self.terminal_stderr = sys.stderr
        sys.stdout = self
        sys.stderr = self

    def write(self, data):
        self.terminal_stdout.write(data)
        self.file.write(data)
        self.flush()

    def flush(self):
        self.terminal_stdout.flush()
        self.file.flush()

    def close(self):
        sys.stdout = self.terminal_stdout
        sys.stderr = self.terminal_stderr
        if not self.file.closed:
            self.file.close()


# Ativa o redirecionamento para o terminal e para o arquivo 'saida.txt'
logger = TeeLogger(LOG_FILE)

# Detecta automaticamente o Python dentro da pasta .venv da raiz do projeto
VENV_PYTHON = BASE_DIR / ".venv" / ("Scripts/python.exe" if os.name == "nt" else "bin/python3")

if VENV_PYTHON.exists():
    PYTHON_EXEC = str(VENV_PYTHON)
    print(f"🐍 Utilizando o ambiente virtual (.venv): {PYTHON_EXEC}")
else:
    PYTHON_EXEC = sys.executable
    print(f"⚠️ Ambiente virtual .venv não localizado. Utilizando Python do sistema: {PYTHON_EXEC}")

# Mapeamento dos serviços
SERVICOS = [
    # 1. Processador de Eventos Compostos (Consumidor/Produtor Crítico)
    ("Processador de Eventos Compostos", "consumidor_e_produtor_01/processador_eventos_compostos.py", [PYTHON_EXEC]),
    
    # 2. Consumidores e Monitores Individuais
    ("Consumidor 01 - Volatilidade", "consumidor_01/consumidor_volatidade.py", [PYTHON_EXEC]),
    ("Consumidor 02 - Cruzamento de Médias", "consumidor_02/consumidor_cruzamento_medias.py", [PYTHON_EXEC]),
    ("Consumidor 03 - Z-Score", "consumidor_03/consumidor_z-score.py", [PYTHON_EXEC]),
    ("Consumidor 04 - Eventos Compostos", "consumidor_04/consumidor_eventos_compostos.py", [PYTHON_EXEC]),
    
    # 3. Produtor Principal de Cotações (Ingestão de Mercado)
    ("Produtor Crypto", "produtor_01/produtor_crypto.py", [PYTHON_EXEC]),

    # 4. Dashboards (Executados via Streamlit através da .venv)
    ("Dashboard Volatilidade", "consumidor_01/dashboard_volatilidade.py", [PYTHON_EXEC, "-m", "streamlit", "run"]),
    ("Dashboard Cruzamento Médias", "consumidor_02/dashboard_cruzamento_medias.py", [PYTHON_EXEC, "-m", "streamlit", "run"]),
    ("Dashboard Z-Score", "consumidor_03/dashboard_zscore.py", [PYTHON_EXEC, "-m", "streamlit", "run"]),
    ("Dashboard Crypto Geral", "consumidor_04/dashboard_crypto.py", [PYTHON_EXEC, "-m", "streamlit", "run"]),
]

# Lista para acompanhar os subprocessos ativos
processos_ativos = []


def log(mensagem: str, tipo: str = "INFO"):
    icones = {"INFO": "ℹ️", "SUCCESS": "✅", "WARN": "⚠️", "ERROR": "❌", "CRIT": "🔥"}
    print(f"{icones.get(tipo, '👉')} [{tipo}] {mensagem}")


def monitorar_saida_subprocesso(proc):
    """Lê continuamente a saída dos subprocessos para registrar no terminal e no arquivo."""
    try:
        for line in iter(proc.stdout.readline, ''):
            if line:
                sys.stdout.write(line)
                sys.stdout.flush()
    except Exception:
        pass
    finally:
        if proc.stdout:
            proc.stdout.close()


def verificar_docker_daemon():
    """Verifica se o serviço do Docker está em execução."""
    log("Verificando se o Docker Daemon está ativo...")
    try:
        subprocess.run(["docker", "info"], check=True, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        log("Docker Daemon está rodando normalmente.", "SUCCESS")
    except (subprocess.CalledProcessError, FileNotFoundError):
        log("O Docker não está rodando ou o comando 'docker' não foi encontrado. Inicie o Docker e tente novamente.", "ERROR")
        sys.exit(1)


def derrubar_processos_existentes():
    """Mata execuções antigas dos scripts do projeto para evitar duplicidade."""
    log("Verificando e encerrando processos Python/Streamlit antigos do projeto...")
    for nome, rel_path, _ in SERVICOS:
        script_name = Path(rel_path).name
        comando_kill = f"pkill -f {script_name}" if os.name != "nt" else f"taskkill /f /fi \"COMMANDLINE eq *{script_name}*\""
        try:
            subprocess.run(comando_kill, shell=True, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        except Exception:
            pass
    time.sleep(1)
    log("Limpeza de processos anteriores concluída.", "SUCCESS")


def subir_docker_compose():
    """Inicia ou reinicia a infraestrutura Docker Kafka/ZooKeeper."""
    docker_file = BASE_DIR / "docker-compose.yml"
    if not docker_file.exists():
        log(f"Arquivo '{docker_file}' não encontrado!", "ERROR")
        sys.exit(1)

    log("Subindo a infraestrutura Kafka/ZooKeeper via Docker Compose...")
    try:
        res = subprocess.run(["docker", "compose", "up", "-d"], check=True, cwd=BASE_DIR, capture_output=True, text=True)
        if res.stdout:
            print(res.stdout)
        log("Contêineres Docker inicializados.", "SUCCESS")
    except subprocess.CalledProcessError:
        log("Falha ao executar 'docker compose up'. Tentando com 'docker-compose'...", "WARN")
        try:
            res = subprocess.run(["docker-compose", "up", "-d"], check=True, cwd=BASE_DIR, capture_output=True, text=True)
            if res.stdout:
                print(res.stdout)
            log("Contêineres Docker inicializados.", "SUCCESS")
        except subprocess.CalledProcessError as e:
            log(f"Erro ao subir a infraestrutura Docker: {e}", "ERROR")
            sys.exit(1)


def aguardar_brokers_kafka(brokers=[("localhost", 9092), ("localhost", 9093), ("localhost", 9094)], timeout_segundos=60):
    """Aguarda até que as portas TCP de todos os Brokers Kafka estejam aceitando conexões."""
    log(f"Aguardando os Brokers Kafka responderem nas portas {[b[1] for b in brokers]}...")
    inicio = time.time()
    
    for host, porta in brokers:
        conectado = False
        while not conectado:
            if time.time() - inicio > timeout_segundos:
                log(f"Tempo limite excedido aguardando o Broker Kafka em {host}:{porta}.", "ERROR")
                sys.exit(1)
            try:
                with socket.create_connection((host, porta), timeout=2.0):
                    conectado = True
            except (socket.error, TimeoutError):
                time.sleep(2)
    
    log("Todos os Brokers Kafka estão online e prontos para conexões!", "SUCCESS")


def verificar_arquivos_existentes():
    """Garante que todos os arquivos de script configurados realmente existem antes de rodar."""
    log("Validando estrutura de arquivos do projeto...")
    for nome, rel_path, _ in SERVICOS:
        caminho_completo = BASE_DIR / rel_path
        if not caminho_completo.exists():
            log(f"Arquivo não encontrado: {rel_path} ({nome})", "ERROR")
            sys.exit(1)
    log("Todos os scripts Python e Dashboards foram localizados com sucesso.", "SUCCESS")


def iniciar_servicos():
    """Inicia os consumidores, processadores, produtores e dashboards em subprocessos."""
    log("Iniciando serviços da aplicação em segundo plano...")

    for nome, rel_path, cmd_base in SERVICOS:
        caminho_script = BASE_DIR / rel_path
        diretorio_trabalho = caminho_script.parent
        
        comando = cmd_base + [str(caminho_script)]

        log(f"Iniciando: {nome} [{rel_path}]")
        
        proc = subprocess.Popen(
            comando,
            cwd=diretorio_trabalho,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            bufsize=1,
            encoding="utf-8",
            errors="replace"
        )
        processos_ativos.append((nome, proc))

        # Thread em segundo plano para ler logs do subprocesso e gravar no arquivo/terminal
        t = threading.Thread(target=monitorar_saida_subprocesso, args=(proc,), daemon=True)
        t.start()

        time.sleep(1.5)

    log("Todos os serviços foram disparados com sucesso!", "SUCCESS")


def encerrar_tudo(signal_received=None, frame=None):
    """Encerra todos os subprocessos e pergunta sobre o Docker."""
    print("\n")
    log("Solicitação de encerramento recebida. Finalizando processos...", "WARN")

    for nome, proc in processos_ativos:
        if proc.poll() is None:
            log(f"Encerrando {nome} (PID: {proc.pid})...")
            proc.terminate()
            try:
                proc.wait(timeout=3)
            except subprocess.TimeoutExpired:
                proc.kill()

    log("Todos os processos Python e Streamlit foram finalizados.", "SUCCESS")
    
    resposta = input("\nDeseja também derrubar os contêineres Docker (docker compose down)? [s/N]: ").strip().lower()
    if resposta == 's':
        log("Derrubando contêineres Docker...")
        res = subprocess.run(["docker", "compose", "down"], cwd=BASE_DIR, capture_output=True, text=True)
        if res.stdout:
            print(res.stdout)
        log("Infraestrutura Docker finalizada.", "SUCCESS")

    logger.close()
    sys.exit(0)


def main():
    signal.signal(signal.SIGINT, encerrar_tudo)
    signal.signal(signal.SIGTERM, encerrar_tudo)

    print("=" * 70)
    print("🚀 ORQUESTRADOR DO SISTEMA DE MONITORAMENTO CRIPTO - KAFKA")
    print(f"📝 Registrando logs em tempo real em: {LOG_FILE}")
    print("=" * 70)

    verificar_docker_daemon()
    verificar_arquivos_existentes()
    derrubar_processos_existentes()
    subir_docker_compose()
    aguardar_brokers_kafka()
    iniciar_servicos()

    print("\n" + "=" * 70)
    log("SISTEMA EM EXECUÇÃO CONTINUA! Pressione Ctrl+C para encerrar tudo.", "SUCCESS")
    print("=" * 70 + "\n")

    try:
        while True:
            time.sleep(5)
            for nome, proc in list(processos_ativos):
                if proc.poll() is not None:
                    log(f"O serviço '{nome}' parou (Código de saída: {proc.returncode}).", "WARN")
                    processos_ativos.remove((nome, proc))
    except KeyboardInterrupt:
        encerrar_tudo()


if __name__ == "__main__":
    main()
