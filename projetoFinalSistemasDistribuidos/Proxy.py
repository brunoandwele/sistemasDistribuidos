import logging
import threading
import time

import zmq

# Configuração global de logging: grava em arquivo e mostra no terminal
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("proxy.log"),
        logging.StreamHandler()
    ]
)

logging.info("Iniciando o proxy ZeroMQ")

# Contexto global para sockets ZeroMQ
context = zmq.Context()

# Sockets principais do proxy

frontend = context.socket(zmq.ROUTER)
frontend.bind("tcp://*:5555")  # Porta de entrada dos clientes

backend = context.socket(zmq.DEALER)
backend.bind("tcp://*:6000")  # Porta de entrada dos servidores

control = context.socket(zmq.REP)
control.bind("tcp://*:6001")  # Canal de controle para comandos de registro, eleição etc

notification_pub = context.socket(zmq.PUB)
notification_pub.bind("tcp://*:6010")  # Canal de publicação para notificações e sync clock

heartbeat_pull = context.socket(zmq.PULL)
heartbeat_pull.bind("tcp://*:6015")  # Recebe heartbeats dos servidores

# Variáveis globais do proxy
server_id_counter = 1
server_registry = {}  # Mapeia ID do servidor para suas informações
last_heartbeat = {}  # Marca o timestamp do último heartbeat recebido de cada servidor
lock = threading.Lock()  # Garante sincronização entre as threads

HEARTBEAT_TIMEOUT = 4  # Tempo máximo (segundos) sem heartbeat para considerar servidor offline


def verify_active_servers():
    """
    Thread que monitora heartbeats recebidos dos servidores.
    Se um servidor não enviar heartbeat dentro do intervalo HEARTBEAT_TIMEOUT,
    ele é removido do registro e considerado offline.
    """
    global server_registry
    while True:
        try:
            # Recebe heartbeat sem bloquear o loop (non-blocking)
            msg = heartbeat_pull.recv_string(flags=zmq.NOBLOCK)
            _, server_id = msg.split()
            with lock:
                last_heartbeat[server_id] = time.time()
            logging.info(f"[PROXY] Heartbeat recebido do servidor {server_id} às {last_heartbeat[server_id]:.2f}")
        except zmq.Again:
            # Nenhum heartbeat no momento, segue para checar timeout
            pass
        except Exception as e:
            logging.error(f"[PROXY] Erro ao receber heartbeat: {e}", exc_info=True)

        # Checagem de servidores "mortos"
        now = time.time()
        with lock:
            for sid in list(last_heartbeat.keys()):
                elapsed = now - last_heartbeat[sid]
                if elapsed > HEARTBEAT_TIMEOUT:
                    logging.warning(f"[PROXY] Servidor {sid} está OFFLINE (sem heartbeat há {elapsed:.1f}s)")
                    last_heartbeat.pop(sid, None)
                    # Remove também do registry
                    removed = server_registry.pop(sid, None)
                    if removed is not None:
                        logging.info(f"[PROXY] Servidor {sid} removido do registry")
                    else:
                        logging.info(f"[PROXY] Servidor {sid} já não estava no registry")
        time.sleep(1)  # Evita busy waiting


def control_thread():
    """
    Thread responsável por processar comandos administrativos vindos do canal de controle (porta 6001).
    Implementa: registro de servidores, listagem, eleição de líder, sincronização de clock e envio de notificações.
    """
    global server_id_counter
    logging.info("Thread de controle de registro de servidores iniciada (porta 6001)")
    while True:
        try:
            msg = control.recv_json()
            logging.info(f"Controle recebeu: {msg}")

            # Registro de novo servidor e atribuição de ID único
            if msg.get("action") == "get_server_id":
                with lock:
                    new_id = server_id_counter
                    server_registry[str(new_id)] = {"id": new_id}
                    server_id_counter += 1
                control.send_json({"server_id": new_id})
                logging.info(f"Novo servidor registrado com ID: {new_id}")

            # Listagem dos servidores atualmente registrados
            elif msg.get("action") == "list_servers":
                with lock:
                    active_servers = list(server_registry.keys())
                control.send_json({"servers": active_servers})
                logging.info(f"Lista de servidores retornada: {active_servers}")

            # Descobre qual é o líder atual (usando maior ID ativo)
            elif msg.get("action") == "who_is_leader":
                with lock:
                    if server_registry:
                        leader_id = max(map(int, server_registry.keys()))
                    else:
                        leader_id = None
                control.send_json({"leader_id": leader_id})
                logging.info(f"[SYNC] Pedido de eleição: líder atual é {leader_id}")

            # Broadcast para sincronização de relógio (clock sync)
            elif msg.get("action") == "sync_clock":
                timestamp = msg.get("timestamp")
                # Envia para todos os servidores via PUB/SUB, tópico 'clock_sync'
                notification_pub.send_string(f"clock_sync {timestamp}")
                logging.info(f"[SYNC] Broadcast de clock_sync enviado: {timestamp}")
                control.send_json({"status": "clock_sync_broadcasted", "timestamp": timestamp})

            # Broadcast de notificação para seguidores de um usuário após novo post
            elif msg.get("action") == "notify_users":
                post_owner = msg.get("post_owner")
                users_to_notify = msg.get("users_to_notify")
                notification_msg = msg.get("msg", f"Novo post de {post_owner} disponível!")

                for user_id, topic in users_to_notify.items():
                    try:
                        # Mensagem no formato "<topic> <mensagem>"
                        full_message = f"{topic} {notification_msg}"
                        notification_pub.send_string(full_message)
                        logging.info(f"Notificação enviada no tópico {topic}: {notification_msg}")
                    except Exception as e:
                        logging.error(f"Erro ao enviar notificação para {topic}: {e}")
                # Confirmação para o servidor solicitante
                control.send_json({"status": "ok", "notified_users": list(users_to_notify.keys())})

            # Qualquer comando desconhecido é reportado como erro
            else:
                control.send_json({"error": "Ação desconhecida"})
                logging.warning(f"Ação desconhecida no canal de controle: {msg}")
        except Exception as e:
            logging.error(f"Erro no canal de controle: {e}", exc_info=True)


# Inicia as threads auxiliares para controle e heartbeat
threading.Thread(target=control_thread, daemon=True).start()
threading.Thread(target=verify_active_servers, daemon=True).start()

logging.info(
    "Sockets ligados: frontend na porta 5555, backend na porta 6000, controle na porta 6001, PUB na 6010, heartbeats na 6015")
print("Proxy iniciado: clientes -> 5555, servidores -> 6000, controle -> 6001, pub -> 6010, heartbeats -> 6015")

logging.info("Iniciando o proxy principal")
try:
    # Função de proxy principal do ZeroMQ: faz a ponte entre frontend (clientes) e backend (servidores)
    zmq.proxy(frontend, backend)
except Exception:
    logging.error("Erro no proxy", exc_info=True)

logging.info("Proxy finalizado")
