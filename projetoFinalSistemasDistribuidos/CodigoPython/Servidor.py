import json
import logging
import random
import threading
import time
import traceback

import zmq

import ReturnCodes

# Configuração inicial do sistema de logging para saída no terminal
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler()  # Provisório: terminal
    ]
)

# Contexto global do ZeroMQ para criação dos sockets
context = zmq.Context()

# Sockets principais utilizados para comunicação entre os componentes do sistema distribuído
mainSocket = context.socket(zmq.REP)
mainSocket.connect("tcp://localhost:6000")  # Comunicação com o proxy principal

dataBaseSocket = context.socket(zmq.REQ)
dataBaseSocket.connect("tcp://localhost:6011")  # Comunicação com o banco de dados central

control_socket = context.socket(zmq.REQ)
control_socket.connect("tcp://localhost:6001")  # Canal de controle e coordenação

heartbeat_push = context.socket(zmq.PUSH)
heartbeat_push.connect("tcp://localhost:6015")  # Canal para envio de heartbeats ao proxy

clock_sync_sub = context.socket(zmq.SUB)
clock_sync_sub.connect("tcp://localhost:6010")  # Canal de sincronização de clock
clock_sync_sub.setsockopt_string(zmq.SUBSCRIBE, "clock_sync")

# Variável global que representa o relógio lógico local deste servidor
local_clock = time.time()


def clock_sync_listener():
    """
    Thread responsável por ouvir mensagens de sincronização de clock
    e ajustar o relógio local conforme o timestamp recebido do líder.
    """
    global local_clock
    while True:
        try:
            msg = clock_sync_sub.recv_string()
            _, timestamp = msg.split()
            timestamp = float(timestamp)
            logging.info(
                f"[SYNC] Sincronização recebida. Ajustando relógio local de {local_clock:.2f} para {timestamp:.2f}")
            local_clock = timestamp
        except Exception as e:
            logging.error(f"[SYNC] Erro ao ajustar relógio: {e}")


def election_and_clock_sync():
    """
    Thread que periodicamente consulta quem é o líder do sistema.
    Se este servidor for o líder, faz o broadcast da sincronização do clock para os demais.
    """
    global server_id, local_clock
    while True:
        time.sleep(12)  # Sincroniza a cada 12 segundos

        try:
            # Descobre o líder atual pelo proxy de controle
            control_socket.send_json({"action": "who_is_leader"})
            response = control_socket.recv_json()
            leader_id = response["leader_id"]
            logging.info(f"[SYNC] Checagem de líder: líder atual é {leader_id}")

            # Apenas o líder faz o broadcast da hora
            if str(server_id) == str(leader_id):
                now = time.time()
                logging.info(f"[SYNC] Sou o líder ({server_id}). Enviando sincronização de relógio ({now:.2f})")
                control_socket.send_json({"action": "sync_clock", "timestamp": now})
                reply = control_socket.recv_json()
                logging.info(f"[SYNC] Resposta do proxy para sync_clock: {reply}")
        except Exception as e:
            logging.error(f"[SYNC] Erro na eleição/sincronização: {e}")


def print_local_clock():
    """
    Thread periódica para logar o valor do relógio local.
    Útil para monitoramento e debugging da sincronização.
    """
    global local_clock
    while True:
        logging.info(f"[SYNC] Relógio local: {local_clock:.2f}")
        time.sleep(10)


def drift_local_clock():
    """
    Thread para simular o drift natural de relógios em sistemas distribuídos.
    Periodicamente incrementa ou decrementa o relógio local.
    """
    global local_clock
    while True:
        # Drift entre -1s e +1s a cada 5 segundos (simulação de imprecisão)
        drift = random.uniform(-1, 1)
        local_clock += drift
        logging.info(f"[DRIFT] Drift aplicado ao relógio local: {drift:+.2f}s. Novo valor: {local_clock:.2f}")
        time.sleep(5)


def handle_sign_up(package):
    """
    Processa o cadastro de novo usuário.
    Repassa os dados para o banco central e retorna o resultado.
    """
    logging.info("Entrando em handle_sign_up")
    logging.debug(f"Pacote recebido em handle_sign_up: {package}")
    username = package["username"]

    # Monta e envia requisição para o banco de dados
    request = {
        "action": "add_user",
        "username": username
    }
    logging.info(f"Enviando requisição ao banco: {request}")
    dataBaseSocket.send_json(request)

    # Recebe e trata resposta do banco
    response = dataBaseSocket.recv_json()
    logging.info(f"Resposta do banco recebida: {response}")
    ret = response["ret"]
    userId = response["id"]
    userTopic = response["topic"]

    if ret == ReturnCodes.SUCCESS:
        logging.info(f"Usuário '{username}' cadastrado com ID {userId}, tópico: {userTopic}")
    else:
        logging.warning(f"Tentativa de cadastro com username já existente: '{username}'")

    response_json = json.dumps({
        "ret": ret,
        "id": userId,
        "topic": userTopic
    })
    logging.info(f"Saindo de handle_sign_up com resposta: {response_json}")
    return response_json


def handle_follow(package):
    """
    Processa solicitação de seguir outro usuário.
    Repassa requisição ao banco e retorna status.
    """
    logging.info("Entrando em handle_follow")
    logging.debug(f"Pacote recebido em handle_follow: {package}")
    followRequestJson = package

    primaryUserId = followRequestJson["id"]
    userToFollow = followRequestJson["to_follow"]

    # Solicita ao banco de dados para adicionar o seguidor
    request = {
        "action": "add_follower",
        "id": primaryUserId,
        "to_follow": userToFollow
    }
    logging.info(f"Enviando requisição ao banco: {request}")
    dataBaseSocket.send_json(request)
    response = dataBaseSocket.recv_json()
    logging.info(f"Resposta do banco recebida: {response}")
    ret = response["ret"]

    if ret == ReturnCodes.SUCCESS:
        logging.info(f"Usuário ID {primaryUserId} começou a seguir '{userToFollow}'")
    elif ret == ReturnCodes.ERROR_INVALID_PARAMETER:
        logging.warning(f"Usuário {primaryUserId} não pode seguir a ele mesmo")
    else:
        logging.warning(f"Usuário para seguir não encontrado: '{userToFollow}' (solicitado por ID {primaryUserId})")

    response_json = json.dumps({"ret": ret})
    logging.info(f"Saindo de handle_follow com resposta: {response_json}")
    return response_json


def notify_followers(userId, username):
    """
    Notifica todos os seguidores de um usuário sobre uma nova postagem.
    Para cada seguidor, recupera o tópico de notificação e repassa para o proxy.
    """
    logging.info(f"Entrando em notify_followers para usuário {username} (ID {userId})")
    # 1. Busca os seguidores do usuário no banco
    request = {
        "action": "get_followers",
        "id": userId
    }
    logging.info(f"Enviando requisição ao banco: {request}")
    dataBaseSocket.send_json(request)
    response = dataBaseSocket.recv_json()
    logging.info(f"Resposta do banco recebida: {response}")
    followers = response["followers"]

    users_to_notify = dict()

    logging.info(f"Notificando seguidores de '{username}' (ID {userId})")
    for followerId in followers:
        # 2. Busca o tópico de cada seguidor
        request_topic = {
            "action": "get_user_topic",
            "id": followerId
        }
        logging.info(f"Enviando requisição ao banco: {request_topic}")
        dataBaseSocket.send_json(request_topic)
        topicResponse = dataBaseSocket.recv_json()
        logging.info(f"Resposta do banco recebida: {topicResponse}")
        topic = topicResponse["topic"]

        users_to_notify[followerId] = topic  # Associa cada seguidor ao seu tópico

    # Monta o pacote para notificação via proxy
    notify_action_request = {
        "action": "notify_users",
        "post_owner": username,
        "users_to_notify": users_to_notify,
        "msg": f"Novo post do {username} disponível!"
    }
    logging.info(f"Enviando pacote de notificação ao proxy: {notify_action_request}")

    # Envia para o proxy pelo canal de controle
    control_socket.send_json(notify_action_request)
    proxy_response = control_socket.recv_json()  # Aguarda resposta do proxy
    logging.info(f"Resposta do proxy após notificação: {proxy_response}")


def handle_receive_posts(package):
    """
    Processa o recebimento de uma nova postagem de usuário.
    Persiste o post no banco central e dispara notificação para seguidores.
    """
    logging.info("Entrando em handle_receive_posts")
    logging.debug(f"Pacote recebido em handle_receive_posts: {package}")
    # Envia post para o banco central
    request = {
        "action": "add_post",
        "post": package
    }
    logging.info(f"Enviando requisição ao banco: {request}")
    dataBaseSocket.send_json(request)

    db_response = dataBaseSocket.recv_json()
    logging.info(f"Resposta do banco recebida: {db_response}")

    userId = package["id"]
    username = package["username"]
    notify_followers(userId, username)

    logging.info(f"Post recebido de '{username}' (ID {userId}): '{package['texto']}'")

    logging.info("Saindo de handle_receive_posts com resposta: Postagem recebida!")
    return "Postagem recebida!"


def handle_send_posts(package):
    """
    Responde à requisição de timeline.
    Busca todos os posts no banco central e retorna para o cliente.
    """
    logging.info("Entrando em handle_send_posts")
    logging.debug(f"Pacote recebido em handle_send_posts: {package}")
    logging.info("Requisição de timeline recebida")
    request = {"action": "get_posts"}
    logging.info(f"Enviando requisição ao banco: {request}")
    dataBaseSocket.send_json(request)
    response = dataBaseSocket.recv_json()
    logging.info(f"Resposta do banco recebida: {response}")
    posts = response["posts"]
    response_encoded = json.dumps(posts).encode('utf-8')
    logging.info(f"Saindo de handle_send_posts com resposta (bytes): {response_encoded}")
    return response_encoded


def add_private_message(privateMessageJson):
    """
    Adiciona uma nova mensagem privada no banco de dados central.
    Valida se remetente e destinatário existem e não são a mesma pessoa.
    """
    logging.info("Entrando em add_private_message")
    logging.debug(f"Pacote recebido em add_private_message: {privateMessageJson}")
    # Monta o request para o banco central
    request = {
        "action": "add_private_message",
        "remetente": privateMessageJson["remetente"],
        "destinatario": privateMessageJson["destinatario"],
        "mensagem": privateMessageJson["mensagem"],
        "timestamp": privateMessageJson["timestamp"]
    }
    logging.info(f"Enviando requisição ao banco: {request}")
    dataBaseSocket.send_json(request)
    response = dataBaseSocket.recv_json()
    logging.info(f"Resposta do banco recebida: {response}")
    ret = response["ret"]

    sender = privateMessageJson["remetente"]
    recipient = privateMessageJson["destinatario"]
    message = privateMessageJson["mensagem"]

    if ret == ReturnCodes.SUCCESS:
        logging.info(f"Mensagem registrada: '{sender}' → '{recipient}': '{message}'")
    elif ret == ReturnCodes.ERROR_INVALID_PARAMETER:
        logging.warning(f"Usuário '{sender}' tentou enviar mensagem para si mesmo.")
    elif ret == ReturnCodes.ERROR_USER_NOT_FOUND:
        logging.warning(f"Remetente ou destinatário inexistente: {sender} → {recipient}")
    else:
        logging.error(f"Falha ao registrar mensagem de '{sender}' para '{recipient}'")

    logging.info(f"Saindo de add_private_message com retorno: {ret}")
    return ret


def handle_private_chat(package):
    """
    Recebe uma mensagem privada de um usuário e a adiciona ao banco.
    Retorna o resultado da operação.
    """
    logging.info("Entrando em handle_private_chat")
    logging.debug(f"Pacote recebido em handle_private_chat: {package}")
    ret = add_private_message(package)
    response = {"ret": ret}
    response_json = json.dumps(response)
    logging.info(f"Saindo de handle_private_chat com resposta: {response_json}")
    return response_json


def handle_show_private_message(requestJson):
    """
    Retorna todas as mensagens privadas entre dois usuários, consultando o banco central.
    """
    logging.info("Entrando em handle_show_private_message")
    logging.debug(f"Pacote recebido em handle_show_private_message: {requestJson}")
    sender = requestJson["remetente"]
    recipient = requestJson["destinatario"]

    # Solicita ao banco central as mensagens privadas dessa conversa
    request = {
        "action": "get_private_messages",
        "remetente": sender,
        "destinatario": recipient
    }
    logging.info(f"Enviando requisição ao banco: {request}")
    dataBaseSocket.send_json(request)
    response = dataBaseSocket.recv_json()
    logging.info(f"Resposta do banco recebida: {response}")

    response_json = json.dumps(response)
    logging.info(f"Saindo de handle_show_private_message com resposta: {response_json}")
    return response_json


def update_list_of_active_servers():
    """
    Atualiza periodicamente a lista de servidores ativos no sistema, consultando o proxy.
    """
    global server_ids
    while True:
        time.sleep(10)
        try:
            control_socket.send_json({"action": "list_servers"})
            active_servers_json = control_socket.recv_json()
            server_ids = active_servers_json["servers"]
            logging.info(f"[Atualização] Servidores conectados: {server_ids}")
        except Exception as e:
            logging.error(f"Erro ao atualizar lista de servidores: {e}")


def send_heartbeat():
    """
    Envia periodicamente mensagens de heartbeat ao proxy para informar que este servidor está ativo.
    """
    global server_id
    while True:
        try:
            heartbeat_push.send_string(f"HEARTBEAT {server_id}")
            logging.info(f"[HEARTBEAT] Enviado heartbeat do server {server_id}")
        except Exception as e:
            logging.error(f"[HEARTBEAT] Erro ao enviar heartbeat do server {server_id}: {e}")
        time.sleep(2)


# ==============================
# Inicialização principal do servidor
# ==============================

# Solicita e armazena o ID único deste servidor junto ao proxy
control_socket.send_json({"action": "get_server_id"})
response = control_socket.recv_json()
server_id = response["server_id"]
logging.info(f"Servidor registrado com ID: {server_id}")

# Obtém a lista inicial de servidores ativos
control_socket.send_json({"action": "list_servers"})
response = control_socket.recv_json()
server_ids = response["servers"]
logging.info(f"Lista de servidores ativos recebida: {server_ids}")

print(f"[Servidor] Recebi meu ID do proxy: {server_id}")

# Após receber o ID, reconfigura o logging para usar arquivo dedicado por servidor
for handler in logging.root.handlers[:]:
    logging.root.removeHandler(handler)

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(f"servidor_{server_id}_log.txt"),
        logging.StreamHandler()
    ]
)
logging.info(f"[LOG] Log individual configurado para servidor_{server_id}_log.txt")

# Inicializa threads de tarefas recorrentes do servidor
threading.Thread(target=update_list_of_active_servers, daemon=True).start()
threading.Thread(target=send_heartbeat, daemon=True).start()
threading.Thread(target=clock_sync_listener, daemon=True).start()
threading.Thread(target=election_and_clock_sync, daemon=True).start()
threading.Thread(target=print_local_clock, daemon=True).start()
threading.Thread(target=drift_local_clock, daemon=True).start()

# ==============================
# Loop principal para processamento de mensagens recebidas dos clientes
# ==============================
while True:
    print("Esperando proxima mensagem")
    message = mainSocket.recv()
    try:
        package = json.loads(message.decode('utf-8'))
        logging.info(f"Mensagem recebida: {package}")
        print("Mensagem recebida: ", package)
        action = package.get("action", "")
        logging.info(f"Processando ação: {action}")
        print(f"Processando ação: {action}")

        # Despacha para o handler correspondente baseado na ação recebida
        if action == "add_user":
            logging.info("Chamando handle_sign_up")
            response = handle_sign_up(package)
            mainSocket.send_string(response)
            logging.info(f"Resposta enviada: {response}")
            print(f"Resposta enviada: {response}")
        elif action == "add_follower":
            logging.info("Chamando handle_follow")
            response = handle_follow(package)
            mainSocket.send_string(response)
            logging.info(f"Resposta enviada: {response}")
            print(f"Resposta enviada: {response}")
        elif action == "post_text":
            logging.info("Chamando handle_receive_posts")
            response = handle_receive_posts(package)
            response_json = json.dumps({"ret": ReturnCodes.SUCCESS, "msg": response})
            mainSocket.send_string(response_json)
            logging.info(f"Resposta enviada: {response_json}")
            print(f"Resposta enviada: {response_json}")
        elif action == "get_timeline":
            logging.info("Chamando handle_send_posts")
            response = handle_send_posts(package)
            mainSocket.send(response)
            logging.info(f"Resposta enviada (bytes): {response}")
            print(f"Resposta enviada (bytes): {response}")
        elif action == "add_private_message":
            logging.info("Chamando handle_private_chat")
            response = handle_private_chat(package)
            mainSocket.send_string(response)
            logging.info(f"Resposta enviada: {response}")
            print(f"Resposta enviada: {response}")
        elif action == "get_private_messages":
            sender = package["remetente"]
            recipient = package["destinatario"]
            # Encaminha diretamente a consulta ao banco, padrão mantido
            request = {
                "action": "get_private_messages",
                "remetente": sender,
                "destinatario": recipient
            }
            logging.info(f"Enviando requisição ao banco: {request}")
            dataBaseSocket.send_json(request)
            db_response = dataBaseSocket.recv_json()
            logging.info(f"Resposta do banco recebida: {db_response}")
            response_json = json.dumps(db_response)
            mainSocket.send_string(response_json)
            logging.info(f"Resposta enviada: {response_json}")
            print(f"Resposta enviada: {response_json}")
        else:
            # Tratamento para ações não reconhecidas
            response_json = json.dumps({"ret": -99, "msg": "Ação desconhecida"})
            mainSocket.send_string(response_json)
            logging.info(f"Resposta enviada: {response_json}")
            print(f"Resposta enviada: {response_json}")

    except Exception as e:
        # Tratamento genérico de exceções
        error_msg = f"Erro: {e}"
        mainSocket.send_string(json.dumps({"ret": -1, "msg": error_msg}))
        logging.error(f"Exceção capturada: {error_msg}\n{traceback.format_exc()}")
        print(f"Erro: {e}")
