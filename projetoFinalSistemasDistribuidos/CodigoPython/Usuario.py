import json
import logging
import threading
from datetime import datetime, timedelta
from queue import Queue

import zmq

from CodigoPython import ReturnCodes


class User:
    """
    Representa um usuário da rede social distribuída.
    Gerencia conexões, ações do usuário (seguir, postar, enviar mensagens privadas),
    e tratamento de notificações em tempo real via PUB/SUB.
    """

    def __init__(self, username):
        """
        Inicializa um novo usuário, registra no sistema distribuído e inicia thread para notificações.
        """
        self.username = username

        # Criação dos sockets do usuário (REQ para comandos, SUB para notificações)
        self.context = zmq.Context()
        self.reqSocket = self.context.socket(zmq.REQ)
        self.reqSocket.connect("tcp://localhost:5555")

        self.notificationSocket = self.context.socket(zmq.SUB)
        self.notificationSocket.connect("tcp://localhost:6010")

        self.followedUsers = list()  # Lista de usernames seguidos
        self.notificationQueue = Queue()  # Fila local de notificações recebidas

        self.userId = 0  # Será atribuído após cadastro
        self.notifyTopic = None  # Tópico PUB/SUB usado para notificações
        self.forcedDelay = 0  # Atraso artificial para simulação de clocks defasados

        self.sign_up()
        self.start_threads()

    def sign_up(self):
        """
        Realiza o cadastro do usuário junto ao servidor.
        Em caso de conflito de username, solicita um novo.
        """
        while True:
            package = {
                "action": "add_user",
                "username": self.username
            }
            serialized_package = json.dumps(package).encode('utf-8')
            self.reqSocket.send(serialized_package)

            signupResponseBytes = self.reqSocket.recv().decode('utf-8')
            signupResponse = json.loads(signupResponseBytes)

            if signupResponse["ret"] == 0:
                # Cadastro bem-sucedido
                self.userId = signupResponse["id"]
                self.notifyTopic = signupResponse["topic"]
                self.notificationSocket.setsockopt_string(zmq.SUBSCRIBE, self.notifyTopic)
                print(
                    f"Usuário '{self.username}' cadastrado com sucesso com ID {self.userId} e tópico '{self.notifyTopic}'.")

                # Configura logging individual para este usuário
                logging.basicConfig(
                    filename=f'{self.username}.log',
                    level=logging.INFO,
                    format='%(asctime)s - %(levelname)s - %(message)s'
                )

                logging.info(
                    f"Usuário '{self.username}' cadastrado com sucesso. ID: {self.userId}, tópico: {self.notifyTopic}")
                break
            else:
                # Username já existe: força o usuário a digitar outro
                print("Username inválido - outro usuário já possui esse username!")
                self.username = input("Informe um novo username: ")

        return

    def wait_for_notify_message(self):
        """
        Thread responsável por receber mensagens do canal de notificações
        (via PUB/SUB) e inserir na fila local para leitura posterior.
        """
        while True:
            try:
                message = self.notificationSocket.recv_string(flags=zmq.NOBLOCK)
                self.notificationQueue.put_nowait(message)
            except zmq.Again:
                continue

    def start_threads(self):
        """
        Inicializa a thread de escuta para notificações (daemon).
        """
        notifyThread = threading.Thread(target=self.wait_for_notify_message, daemon=True)
        notifyThread.start()

    def follow(self, user):
        """
        (Não usado diretamente: placeholder para um possível sistema mais complexo de seguimento local.)
        """
        self.followedUsers.append(user)

    def __str__(self):
        """
        Retorna representação amigável do usuário.
        """
        return f"Usuário: {self.username}"

    def post_text(self):
        """
        Permite ao usuário publicar um texto na rede social.
        O timestamp pode ser atrasado artificialmente para testes de sincronização.
        """
        print("\n--- Publicar Texto ---")
        text = input("Digite seu texto: ")

        messageTimestamp = datetime.now() - timedelta(seconds=self.forcedDelay)

        messagePayload = {
            "action": "post_text",
            "username": self.username,
            "id": self.userId,
            "texto": text,
            "tempoEnvioMensagem": messageTimestamp.isoformat()
        }

        serializedMessage = json.dumps(messagePayload).encode('utf-8')

        self.reqSocket.send(serializedMessage)

        response = self.reqSocket.recv()
        logging.info(f"Usuário '{self.username}' publicou um texto: '{text}'")

    def view_timeline(self):
        """
        Solicita e exibe a timeline do usuário, mostrando todos os posts disponíveis.
        """
        timelineRequestMessage = {
            "action": "get_timeline"
        }
        self.reqSocket.send(json.dumps(timelineRequestMessage).encode('utf-8'))

        print("Enviou mensagem de req")
        print("Esperando resposta")

        serializedPosts = self.reqSocket.recv()
        print("Recebeu postagens")

        posts = json.loads(serializedPosts.decode('utf-8'))

        logging.info(f"Usuário '{self.username}' visualizou a timeline")

        print("\n--- Postagens Recebidas ---")
        for post in posts:
            print("----------------------------------")
            print(f"User: {post['username']}")
            print(f"Texto: {post['texto']}")
            print(f"Enviado em: {post['tempoEnvioMensagem']}")

    def follow_user(self):
        """
        Permite ao usuário seguir outro usuário na rede.
        Trata validação para não seguir a si mesmo e reporta resultado.
        """
        print("\n--- Seguir Usuário ---")
        usernameInput = input("Digite o nome do usuário que deseja seguir: ")

        if usernameInput == self.username:
            print("Você não pode seguir a si mesmo.")
            logging.warning(f"Usuário '{self.username}' tentou seguir a si mesmo.")
            return

        followRequest = {
            "action": "add_follower",
            "id": self.userId,
            "to_follow": usernameInput
        }

        serializedMessage = json.dumps(followRequest).encode('utf-8')
        self.reqSocket.send(serializedMessage)

        responseBytes = self.reqSocket.recv()
        response = json.loads(responseBytes.decode('utf-8'))

        if response["ret"] == ReturnCodes.SUCCESS:
            print(f"Agora você está seguindo {usernameInput}.")
            logging.info(f"Usuário '{self.username}' seguiu o usuário '{usernameInput}'")
            self.followedUsers.append(usernameInput)
        elif response["ret"] == ReturnCodes.ERROR_USER_NOT_FOUND:
            print("Usuário não encontrado.")
            logging.warning(f"Usuário '{usernameInput}' não encontrado para seguir por '{self.username}'")
        elif response["ret"] == ReturnCodes.ERROR_INVALID_PARAMETER:
            logging.warning(f"Usuário '{usernameInput}' não pode seguir a ele mesmo")
        else:
            print("Erro ao tentar seguir o usuário.")
            logging.error(f"Erro ao seguir usuário '{usernameInput}' por '{self.username}'")



    def send_private_message(self):
        """
        Permite enviar uma mensagem privada para outro usuário.
        Também exibe a conversa antes do envio para dar contexto ao usuário.
        """
        print("\n--- Enviar Mensagem Privada ---")
        sender = self.username
        recipient = input("Digite o nome do destinatário: ")

        if recipient == sender:
            print("Você não pode enviar mensagens para si mesmo.")
            logging.warning(f"Usuário '{sender}' tentou enviar mensagem para si mesmo.")
            return

        self.display_conversation(sender, recipient)

        message = input("Digite a mensagem: ")
        adjustedTime = datetime.now() - timedelta(seconds=self.forcedDelay)
        timestamp = str(int(adjustedTime.timestamp()))
        if self.forcedDelay > 0:
            logging.info(f"Atraso de {self.forcedDelay}s aplicado na mensagem privada.")

        messageRequest = {
            "action": "add_private_message",
            "remetente": sender,
            "destinatario": recipient,
            "mensagem": message,
            "timestamp": timestamp
        }

        serializedMessage = json.dumps(messageRequest).encode('utf-8')
        self.reqSocket.send(serializedMessage)

        responseBytes = self.reqSocket.recv()
        response = json.loads(responseBytes.decode('utf-8'))

        if response["ret"] == ReturnCodes.SUCCESS:
            logging.info(f"Usuário '{sender}' enviou mensagem para '{recipient}': '{message}'")
            self.display_conversation(sender, recipient)
        else:
            print("Erro ao enviar mensagem, tente novamente!")
            logging.error(f"Erro ao enviar mensagem de '{sender}' para '{recipient}'")

    def view_notifications(self):
        """
        Exibe todas as notificações recebidas via PUB/SUB que ainda não foram lidas.
        """
        print("\n--- Ver Notificações ---")

        notifications = []
        while not self.notificationQueue.empty():
            notification = self.notificationQueue.get_nowait()
            notifications.append(notification)

        logging.info(f"Usuário '{self.username}' verificou notificações. Total: {len(notifications)}")

        if not notifications:
            print("Nenhuma nova notificação.")
        else:
            for idx, notification in enumerate(notifications, 1):
                print(f"[{idx}] {notification}")

    def set_forced_delay(self):
        """
        Permite ao usuário definir um atraso artificial no relógio local,
        simulando clocks defasados em ambiente distribuído.
        """
        print("\n--- Configurar Atraso Forçado ---")
        try:
            delay = int(input("Digite o atraso em segundos (0 para nenhum): "))
            self.forcedDelay = delay
            print(f"Atraso forçado configurado para {delay} segundos.")
            logging.info(f"Usuário '{self.username}' configurou atraso forçado para {delay} segundos.")
        except ValueError:
            print("Valor inválido. Digite um número inteiro.")


def show_menu():
    """
    Exibe o menu principal de opções da aplicação de usuário.
    """
    print("\n===== Menu da Rede Social =====")
    print("1. Publicar texto")
    print("2. Seguir usuário")
    print("3. Enviar mensagem privada")
    print("4. Ver notificações")
    print("5. Ver timeline")
    print("6. Forçar atraso no relógio")
    print("7. Sair")


def main_menu():
    """
    Loop principal do cliente. Cria usuário e despacha para cada funcionalidade via menu.
    """
    usernameInput = input("Digite seu nome de usuário: ")
    user = User(usernameInput)

    while True:
        show_menu()
        try:
            option = int(input("Escolha uma opção: "))
        except ValueError:
            print("Por favor, digite um número válido.")
            continue

        if option == 1:
            user.post_text()
        elif option == 2:
            user.follow_user()
        elif option == 3:
            user.send_private_message()
        elif option == 4:
            user.view_notifications()
        elif option == 5:
            user.view_timeline()
        elif option == 6:
            user.set_forced_delay()
        elif option == 7:
            print("Saindo...")
            break
        else:
            print("Opção inválida. Tente novamente.")


if __name__ == "__main__":
    main_menu()
