#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <jansson.h>
#include <zmq.h>
#include <pthread.h>
#include <time.h>
#include <unistd.h>

#define MAX_USERNAME 64
#define MAX_TEXT 512
#define MAX_NOTIFICATION 1024
#define MAX_NOTIFICATIONS 100

typedef struct {
    char username[MAX_USERNAME];
    int userId;
    char notifyTopic[128];
    int forcedDelay;
    void *context;
    void *reqSocket;
    void *notificationSocket;
    pthread_t notifyThread;
    char notifications[MAX_NOTIFICATIONS][MAX_NOTIFICATION];
    int notificationCount;
    pthread_mutex_t notif_mutex;
    FILE *logfile;
} User;

// Utilitário para registrar log com timestamp
void write_log(User *user, const char *level, const char *msg) {
    if (!user->logfile) return;
    time_t now = time(NULL);
    struct tm *lt = localtime(&now);
    char timestr[64];
    strftime(timestr, sizeof(timestr), "%Y-%m-%d %H:%M:%S", lt);
    fprintf(user->logfile, "%s - %s - %s\n", timestr, level, msg);
    fflush(user->logfile);
}

// NOTIFICATION LISTENER THREAD
void* notification_listener(void *arg) {
    User *user = (User*)arg;
    while (1) {
        zmq_msg_t msg;
        zmq_msg_init(&msg);
        int size = zmq_msg_recv(&msg, user->notificationSocket, 0);
        if (size > 0) {
            pthread_mutex_lock(&user->notif_mutex);
            if (user->notificationCount < MAX_NOTIFICATIONS) {
                snprintf(user->notifications[user->notificationCount++], MAX_NOTIFICATION, "%s", (char*)zmq_msg_data(&msg));
            }
            pthread_mutex_unlock(&user->notif_mutex);
        }
        zmq_msg_close(&msg);
    }
    return NULL;
}

// USER SIGNUP (REGISTRO)
int user_signup(User *user) {
    while (1) {
        json_t *obj = json_object();
        json_object_set_new(obj, "action", json_string("add_user"));
        json_object_set_new(obj, "username", json_string(user->username));
        char *serialized = json_dumps(obj, 0);

        zmq_send(user->reqSocket, serialized, strlen(serialized), 0);

        char buffer[1024];
        int recv_size = zmq_recv(user->reqSocket, buffer, sizeof(buffer) - 1, 0);
        if (recv_size < 0) {
            free(serialized);
            json_decref(obj);
            return 0;
        }
        buffer[recv_size] = 0;

        json_error_t error;
        json_t *reply = json_loads(buffer, 0, &error);

        if (json_integer_value(json_object_get(reply, "ret")) == 0) {
            user->userId = json_integer_value(json_object_get(reply, "id"));
            snprintf(user->notifyTopic, sizeof(user->notifyTopic), "%s", json_string_value(json_object_get(reply, "topic")));
            printf("Usuário '%s' cadastrado! ID=%d, tópico='%s'\n", user->username, user->userId, user->notifyTopic);

            zmq_setsockopt(user->notificationSocket, ZMQ_SUBSCRIBE, user->notifyTopic, strlen(user->notifyTopic));

            // LOG INDIVIDUAL
            char logfname[MAX_USERNAME + 8];
            snprintf(logfname, sizeof(logfname), "%s.log", user->username);
            user->logfile = fopen(logfname, "a");
            if (!user->logfile) {
                printf("Erro ao criar arquivo de log!\n");
                exit(1);
            }
            char logmsg[256];
            snprintf(logmsg, sizeof(logmsg),
                     "Usuário '%s' cadastrado com sucesso. ID: %d, tópico: %s",
                     user->username, user->userId, user->notifyTopic);
            write_log(user, "INFO", logmsg);

            json_decref(reply);
            free(serialized);
            json_decref(obj);
            return 1;
        } else {
            printf("Username inválido - outro usuário já possui esse username!\nInforme um novo username: ");
            scanf("%s", user->username);
        }
        json_decref(reply);
        free(serialized);
        json_decref(obj);
    }
}

// POSTAR TEXTO
void post_text(User *user) {
    char text[MAX_TEXT];
    printf("\n--- Publicar Texto ---\n");
    printf("Digite seu texto: ");
    getchar(); // Limpa buffer pendente do scanf anterior
    fgets(text, sizeof(text), stdin);
    text[strcspn(text, "\n")] = 0; // Remove newline

    time_t now = time(NULL) - user->forcedDelay;
    struct tm *lt = localtime(&now);
    char timestamp[32];
    strftime(timestamp, sizeof(timestamp), "%Y-%m-%dT%H:%M:%S", lt);

    json_t *msg = json_object();
    json_object_set_new(msg, "action", json_string("post_text"));
    json_object_set_new(msg, "username", json_string(user->username));
    json_object_set_new(msg, "id", json_integer(user->userId));
    json_object_set_new(msg, "texto", json_string(text));
    json_object_set_new(msg, "tempoEnvioMensagem", json_string(timestamp));

    char *serialized = json_dumps(msg, 0);

    zmq_send(user->reqSocket, serialized, strlen(serialized), 0);
    zmq_recv(user->reqSocket, NULL, 0, 0);

    printf("Texto publicado!\n");

    char logmsg[MAX_TEXT + 128];
    snprintf(logmsg, sizeof(logmsg),
             "Usuário '%s' publicou um texto: '%s'", user->username, text);
    write_log(user, "INFO", logmsg);

    free(serialized);
    json_decref(msg);
}

// SEGUIR USUÁRIO
void follow_user(User *user) {
    char to_follow[MAX_USERNAME];
    printf("\n--- Seguir Usuário ---\n");
    printf("Digite o nome do usuário que deseja seguir: ");
    scanf("%s", to_follow);

    if (strcmp(to_follow, user->username) == 0) {
        printf("Você não pode seguir a si mesmo.\n");
        write_log(user, "WARNING", "Tentativa de seguir a si mesmo.");
        return;
    }

    json_t *req = json_object();
    json_object_set_new(req, "action", json_string("add_follower"));
    json_object_set_new(req, "id", json_integer(user->userId));
    json_object_set_new(req, "to_follow", json_string(to_follow));

    char *serialized = json_dumps(req, 0);
    zmq_send(user->reqSocket, serialized, strlen(serialized), 0);

    char buffer[1024];
    int recv_size = zmq_recv(user->reqSocket, buffer, sizeof(buffer) - 1, 0);
    buffer[recv_size] = 0;
    json_error_t error;
    json_t *resp = json_loads(buffer, 0, &error);

    if (json_integer_value(json_object_get(resp, "ret")) == 0) {
        printf("Agora você está seguindo %s.\n", to_follow);
        char logmsg[MAX_USERNAME + 64];
        snprintf(logmsg, sizeof(logmsg), "Usuário '%s' seguiu o usuário '%s'", user->username, to_follow);
        write_log(user, "INFO", logmsg);
    } else if (json_integer_value(json_object_get(resp, "ret")) == 2) {
        printf("Usuário não encontrado.\n");
        char logmsg[MAX_USERNAME + 64];
        snprintf(logmsg, sizeof(logmsg), "Usuário '%s' não encontrado para seguir por '%s'", to_follow, user->username);
        write_log(user, "WARNING", logmsg);
    } else {
        printf("Erro ao seguir usuário.\n");
        write_log(user, "ERROR", "Erro ao seguir usuário.");
    }
    json_decref(resp);
    free(serialized);
    json_decref(req);
}

// MOSTRAR CONVERSAA

void display_conversation(User *user, const char *recipient) {
    json_t *req = json_object();
    json_object_set_new(req, "action", json_string("get_private_messages"));
    json_object_set_new(req, "remetente", json_string(user->username));
    json_object_set_new(req, "destinatario", json_string(recipient));

    char *serialized = json_dumps(req, 0);
    zmq_send(user->reqSocket, serialized, strlen(serialized), 0);

    char buffer[16384];
    int recv_size = zmq_recv(user->reqSocket, buffer, sizeof(buffer) - 1, 0);
    buffer[recv_size] = 0;

    json_error_t error;
    json_t *resp = json_loads(buffer, 0, &error);
    free(serialized);
    json_decref(req);

    printf("\n📱 Conversa entre você e %s\n", recipient);
    printf("--------------------------------------------------\n");

    json_t *messages = json_object_get(resp, "mensagens");
    if (!messages || !json_is_array(messages) || json_array_size(messages) == 0) {
        printf("Nenhuma mensagem até agora.\n");
        json_decref(resp);
        return;
    }

    size_t i;
    for (i = 0; i < json_array_size(messages); ++i) {
        json_t *item = json_array_get(messages, i);
        if (!json_is_array(item) || json_array_size(item) != 3) continue;

        const char *message = json_string_value(json_array_get(item, 0));
        json_int_t ts = json_integer_value(json_array_get(item, 1));
        const char *msgSender = json_string_value(json_array_get(item, 2));

        char timeFormatted[8] = "??:??";
        if (ts > 0) {
            time_t t = (time_t)ts;
            struct tm *tm_info = localtime(&t);
            strftime(timeFormatted, sizeof(timeFormatted), "%H:%M", tm_info);
        }

        if (strcmp(msgSender, user->username) == 0) {
            printf("%25s %s: %s  🕒%s\n", "", msgSender, message, timeFormatted);
        } else {
            printf("%s: %s  🕒%s\n", msgSender, message, timeFormatted);
        }
    }
    json_decref(resp);
}

// ENVIAR MENSAGEM PRIVADA
void send_private_message(User *user) {
    char to_user[MAX_USERNAME];
    char message[MAX_TEXT];

    printf("\n--- Enviar Mensagem Privada ---\n");
    printf("Digite o nome do usuário destino: ");
    scanf("%s", to_user);
    getchar(); // limpa \n

    if (strcmp(to_user, user->username) == 0) {
        printf("Você não pode enviar mensagem para si mesmo!\n");
        write_log(user, "WARNING", "Tentativa de enviar mensagem privada para si mesmo.");
        return;
    }

    display_conversation(user, to_user); // Mostra o histórico antes do envio

    printf("Digite a mensagem: ");
    fgets(message, sizeof(message), stdin);
    message[strcspn(message, "\n")] = 0; // Remove newline

    time_t now = time(NULL) - user->forcedDelay;

    json_t *req = json_object();
    json_object_set_new(req, "action", json_string("add_private_message"));
    json_object_set_new(req, "remetente", json_string(user->username));
    json_object_set_new(req, "destinatario", json_string(to_user));
    json_object_set_new(req, "mensagem", json_string(message));
    json_object_set_new(req, "timestamp", json_integer(now));

    char *serialized = json_dumps(req, 0);
    zmq_send(user->reqSocket, serialized, strlen(serialized), 0);

    char buffer[1024];
    int recv_size = zmq_recv(user->reqSocket, buffer, sizeof(buffer) - 1, 0);
    buffer[recv_size] = 0;
    json_error_t error;
    json_t *resp = json_loads(buffer, 0, &error);

    if (json_integer_value(json_object_get(resp, "ret")) == 0) {
        printf("Mensagem enviada com sucesso para %s!\n", to_user);
        char logmsg[MAX_USERNAME + MAX_TEXT + 64];
        snprintf(logmsg, sizeof(logmsg), "Enviou mensagem privada para '%s': %s", to_user, message);
        write_log(user, "INFO", logmsg);
        display_conversation(user, to_user); // Mostra o histórico após o envio
    } else if (json_integer_value(json_object_get(resp, "ret")) == 2) {
        printf("Usuário de destino não encontrado.\n");
        char logmsg[MAX_USERNAME + 64];
        snprintf(logmsg, sizeof(logmsg), "Tentativa de enviar mensagem privada para usuário inexistente: '%s'", to_user);
        write_log(user, "WARNING", logmsg);
    } else {
        printf("Erro ao enviar mensagem privada.\n");
        write_log(user, "ERROR", "Falha ao enviar mensagem privada.");
    }
    json_decref(resp);
    free(serialized);
    json_decref(req);
}

// NOTIFICAÇÕES
void view_notifications(User *user) {
    printf("\n--- Ver Notificações ---\n");
    pthread_mutex_lock(&user->notif_mutex);
    int total = user->notificationCount;
    if (total == 0) {
        printf("Nenhuma nova notificação.\n");
    } else {
        for (int i = 0; i < total; ++i) {
            printf("[%d] %s\n", i + 1, user->notifications[i]);
        }
        user->notificationCount = 0;
    }
    pthread_mutex_unlock(&user->notif_mutex);

    char logmsg[128];
    snprintf(logmsg, sizeof(logmsg), "Usuário '%s' verificou notificações. Total: %d", user->username, total);
    write_log(user, "INFO", logmsg);
}

// TIMELINE
void view_timeline(User *user) {
    json_t *req = json_object();
    json_object_set_new(req, "action", json_string("get_timeline"));
    char *serialized = json_dumps(req, 0);

    zmq_send(user->reqSocket, serialized, strlen(serialized), 0);

    char buffer[8192];
    int recv_size = zmq_recv(user->reqSocket, buffer, sizeof(buffer) - 1, 0);
    buffer[recv_size] = 0;

    json_error_t error;
    json_t *posts = json_loads(buffer, 0, &error);

    char logmsg[128];
    snprintf(logmsg, sizeof(logmsg), "Usuário '%s' visualizou a timeline", user->username);
    write_log(user, "INFO", logmsg);

    printf("\n--- Postagens Recebidas ---\n");
    if (!json_is_array(posts)) {
        printf("Nenhuma postagem disponível.\n");
    } else {
        size_t index;
        json_t *post;
        json_array_foreach(posts, index, post) {
            printf("----------------------------------\n");
            printf("User: %s\n", json_string_value(json_object_get(post, "username")));
            printf("Texto: %s\n", json_string_value(json_object_get(post, "texto")));
            printf("Enviado em: %s\n", json_string_value(json_object_get(post, "tempoEnvioMensagem")));
        }
    }
    json_decref(posts);
    free(serialized);
    json_decref(req);
}

// ATRASO FORÇADO
void set_forced_delay(User *user) {
    printf("\n--- Configurar Atraso Forçado ---\n");
    printf("Digite o atraso em segundos (0 para nenhum): ");
    scanf("%d", &user->forcedDelay);
    printf("Atraso forçado configurado para %d segundos.\n", user->forcedDelay);

    char logmsg[128];
    snprintf(logmsg, sizeof(logmsg), "Usuário '%s' configurou atraso forçado para %d segundos.", user->username, user->forcedDelay);
    write_log(user, "INFO", logmsg);
}

// MENU
void show_menu() {
    printf("\n===== Menu da Rede Social =====\n");
    printf("1. Publicar texto\n");
    printf("2. Seguir usuário\n");
    printf("3. Enviar mensagem privada\n");
    printf("4. Ver notificações\n");
    printf("5. Ver timeline\n");
    printf("6. Forçar atraso no relógio\n");
    printf("7. Sair\n");
}

// MAIN
int main() {
    User user;
    user.forcedDelay = 0;
    user.userId = 0;
    user.notificationCount = 0;
    user.logfile = NULL;
    pthread_mutex_init(&user.notif_mutex, NULL);

    printf("Digite seu nome de usuário: ");
    scanf("%s", user.username);

    user.context = zmq_ctx_new();
    user.reqSocket = zmq_socket(user.context, ZMQ_REQ);
    zmq_connect(user.reqSocket, "tcp://localhost:5555");
    user.notificationSocket = zmq_socket(user.context, ZMQ_SUB);
    zmq_connect(user.notificationSocket, "tcp://localhost:6010");

    if (!user_signup(&user)) {
        printf("Erro ao cadastrar usuário.\n");
        exit(1);
    }

    pthread_create(&user.notifyThread, NULL, notification_listener, &user);

    int option;
    do {
        show_menu();
        printf("Escolha uma opção: ");
        scanf("%d", &option);
        getchar(); // Limpa \n do teclado

        switch(option) {
            case 1: post_text(&user); break;
            case 2: follow_user(&user); break;
            case 3: send_private_message(&user); break;
            case 4: view_notifications(&user); break;
            case 5: view_timeline(&user); break;
            case 6: set_forced_delay(&user); break;
            case 7: printf("Saindo...\n"); write_log(&user, "INFO", "Sessão encerrada."); break;
            default: printf("Opção inválida. Tente novamente.\n");
        }
    } while(option != 7);

    if (user.logfile)
        fclose(user.logfile);

    zmq_close(user.reqSocket);
    zmq_close(user.notificationSocket);
    zmq_ctx_destroy(user.context);
    pthread_mutex_destroy(&user.notif_mutex);
    return 0;
}