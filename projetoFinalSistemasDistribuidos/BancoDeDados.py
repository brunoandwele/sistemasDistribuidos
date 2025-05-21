import logging
import threading

import zmq

import ReturnCodes

# Configuração global de logging: salva em arquivo e exibe no terminal
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("banco.log"),
        logging.StreamHandler()  # Agora os logs aparecem no terminal em tempo real!
    ]
)
logging.info("Iniciando Banco de Dados...")

# Inicialização do contexto ZeroMQ e criação do socket REP (resposta) na porta 6011
context = zmq.Context()
socket = context.socket(zmq.REP)
socket.bind("tcp://*:6011")

# Estrutura interna: banco de dados em memória simulando as tabelas necessárias
database = {
    "usernames": {},  # username -> id do usuário
    "user_followers": {},  # id do usuário -> lista de ids de seguidores
    "user_topics": {},  # id do usuário -> tópico de notificação (PUB/SUB)
    "posts": [],  # lista de posts (dicionários)
    "private_messages": {}  # remetente -> destinatário -> [[mensagem, timestamp, sender], ...]
}

user_id_counter = 1  # Contador incremental para gerar novos IDs de usuário


def handle_request():
    """
    Função principal que executa o loop de atendimento das requisições recebidas no socket REP.
    Processa todas as ações de CRUD do sistema, incluindo cadastro, postagens, seguidores e mensagens privadas.
    """
    global user_id_counter
    logging.info("Thread handle_request iniciada.")
    while True:
        # Recebe mensagem JSON do socket e identifica a ação solicitada
        message = socket.recv_json()
        logging.info(f"Mensagem recebida: {message}")
        action = message["action"]

        # Cadastro de novo usuário
        if action == "add_user":
            logging.info(f"Processando ação: {action}, dados: {message}")
            username = message["username"]
            if username in database["usernames"]:
                # Username já está em uso
                resposta = {"ret": ReturnCodes.ERROR_USERNAME_TAKEN}
                logging.error(f"Resposta enviada: {resposta}")
                socket.send_json(resposta)
            else:
                # Novo usuário: atribui id, cria estruturas e tópico
                user_id = user_id_counter
                user_id_counter += 1
                database["usernames"][username] = user_id
                database["user_followers"][user_id] = []
                database["user_topics"][user_id] = f"notificacao_user_{user_id}"
                resposta = {
                    "ret": ReturnCodes.SUCCESS,
                    "id": user_id,
                    "topic": database["user_topics"][user_id]
                }
                logging.info(f"Resposta enviada: {resposta}")
                socket.send_json(resposta)

        # Consulta do id de usuário a partir do username
        elif action == "get_user_id":
            logging.info(f"Processando ação: {action}, dados: {message}")
            username = message["username"]
            user_id = database["usernames"].get(username, -1)
            resposta = {"id": user_id}
            logging.info(f"Resposta enviada: {resposta}")
            socket.send_json(resposta)

        # Adiciona novo post e ordena por timestamp, garantindo timeline correta
        elif action == "add_post":
            logging.info(f"Processando ação: {action}, dados: {message}")
            database["posts"].append(message["post"])
            database["posts"].sort(key=lambda x: x["tempoEnvioMensagem"])
            resposta = {"ret": 0}
            logging.info(f"Resposta enviada: {resposta}")
            socket.send_json(resposta)

        # Retorna todos os posts salvos no sistema
        elif action == "get_posts":
            logging.info(f"Processando ação: {action}, dados: {message}")
            resposta = {"posts": database["posts"]}
            logging.info(f"Resposta enviada: {resposta}")
            socket.send_json(resposta)

        # Consulta o tópico de notificação associado a um usuário
        elif action == "get_user_topic":
            logging.info(f"Processando ação: {action}, dados: {message}")
            uid = message["id"]
            topic = database["user_topics"].get(uid, "")
            resposta = {"topic": topic}
            logging.info(f"Resposta enviada: {resposta}")
            socket.send_json(resposta)

        # Adiciona um seguidor a outro usuário
        elif action == "add_follower":
            logging.info(f"Processando ação: {action}, dados: {message}")
            uid = message["id"]
            to_follow = message["to_follow"]
            if uid == to_follow:
                # Não é permitido seguir a si mesmo
                resposta = {"ret": ReturnCodes.ERROR_INVALID_PARAMETER}
                logging.error(f"Resposta enviada: {resposta}")
                socket.send_json(resposta)
            elif to_follow in database["usernames"]:
                # Adiciona uid como seguidor do usuário solicitado
                to_follow_id = database["usernames"][to_follow]
                database["user_followers"][to_follow_id].append(uid)
                resposta = {"ret": ReturnCodes.SUCCESS}
                logging.info(f"Resposta enviada: {resposta}")
                socket.send_json(resposta)
            else:
                # Usuário alvo não existe
                resposta = {"ret": ReturnCodes.ERROR_USER_NOT_FOUND}
                logging.error(f"Resposta enviada: {resposta}")
                socket.send_json(resposta)

        # Retorna todos os seguidores de um usuário
        elif action == "get_followers":
            logging.info(f"Processando ação: {action}, dados: {message}")
            uid = message["id"]
            followers = database["user_followers"].get(uid, [])
            resposta = {"followers": followers}
            logging.info(f"Resposta enviada: {resposta}")
            socket.send_json(resposta)

        # Adiciona mensagem privada entre dois usuários e armazena dos dois lados para consulta bidirecional
        elif action == "add_private_message":
            logging.info(f"Processando ação: {action}, dados: {message}")
            sender = message["remetente"]
            recipient = message["destinatario"]
            msg = message["mensagem"]
            ts = message["timestamp"]

            # Verificação de parâmetros válidos (usuários existentes e diferentes)
            if sender == recipient or sender not in database["usernames"] or recipient not in database["usernames"]:
                resposta = {"ret": ReturnCodes.ERROR_INVALID_PARAMETER}
                logging.error(f"Resposta enviada: {resposta}")
                socket.send_json(resposta)
                continue

            # Armazena a mensagem nos dois sentidos para facilitar consulta
            for a, b in [(sender, recipient), (recipient, sender)]:
                database["private_messages"].setdefault(a, {}).setdefault(b, [])
                database["private_messages"][a][b].append([msg, int(ts), sender])
                database["private_messages"][a][b].sort(key=lambda x: x[1])
            resposta = {"ret": ReturnCodes.SUCCESS}
            logging.info(f"Resposta enviada: {resposta}")
            socket.send_json(resposta)

        # Recupera todas as mensagens privadas entre dois usuários
        elif action == "get_private_messages":
            logging.info(f"Processando ação: {action}, dados: {message}")
            sender = message["remetente"]
            recipient = message["destinatario"]

            msgs = database["private_messages"].get(sender, {}).get(recipient, [])
            resposta = {"ret": 0, "mensagens": msgs}
            logging.info(f"Resposta enviada: {resposta}")
            socket.send_json(resposta)

        # Ação não reconhecida
        else:
            resposta = {"ret": -99, "msg": "Ação não reconhecida"}
            logging.error(f"Resposta enviada: {resposta}")
            socket.send_json(resposta)
    logging.info("Thread handle_request finalizada.")


# Inicia a thread principal do banco de dados em modo daemon (encerra junto com o processo principal)
threading.Thread(target=handle_request, daemon=True).start()

print("ServidorBanco rodando na porta 7000...")
input("Pressione Enter para sair.\n")
