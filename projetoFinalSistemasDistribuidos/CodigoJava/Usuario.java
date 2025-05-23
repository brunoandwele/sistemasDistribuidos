import org.json.JSONArray;
import org.json.JSONObject;
import org.zeromq.ZMQ;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.locks.ReentrantLock;
import java.util.logging.*;
import java.util.logging.Formatter;

public class Usuario {
    private static final int MAX_NOTIFICATIONS = 100;
    private String username;
    private int userId;
    private String notifyTopic;
    private int forcedDelay = 0;
    private List<String> notifications = new ArrayList<>();
    private final ReentrantLock notifLock = new ReentrantLock();
    private Logger logger;
    private Handler logFileHandler;

    // ZMQ context/sockets
    private ZMQ.Context context;
    private ZMQ.Socket reqSocket;
    private ZMQ.Socket notifSocket;
    private Thread notifThread;

    public static void main(String[] args) throws Exception {
        new Usuario().run();
    }

    public void run() throws Exception {
        Scanner scanner = new Scanner(System.in);

        System.out.print("Digite seu nome de usuário: ");
        username = scanner.next();

        // Logger setup (user-specific)
        setupLogger();

        // ZMQ
        context = ZMQ.context(1);
        reqSocket = context.socket(ZMQ.REQ);
        reqSocket.connect("tcp://localhost:5555");
        notifSocket = context.socket(ZMQ.SUB);
        notifSocket.connect("tcp://localhost:6010");

        if (!signupUser(scanner)) {
            System.err.println("Erro ao cadastrar usuário!");
            close();
            return;
        }

        notifThread = new Thread(this::listenNotifications);
        notifThread.setDaemon(true);
        notifThread.start();

        // ==== MENU EXATAMENTE NA ORDEM SOLICITADA ====
        while (true) {
            showMenu();
            System.out.print("Escolha uma opção: ");
            int opt = scanner.nextInt();
            scanner.nextLine(); // consume newline
            switch (opt) {
                case 1:
                    postText(scanner);
                    break;
                case 2:
                    followUser(scanner);
                    break;
                case 3:
                    sendPrivateMessage(scanner);
                    break;
                case 4:
                    showNotifications();
                    break;
                case 5:
                    viewTimeline();
                    break;
                case 6:
                    setForcedDelay(scanner);
                    break;
                case 7:
                    log("INFO", "Sessão encerrada.");
                    System.out.println("Saindo...");
                    close();
                    return;
                default:
                    System.out.println("Opção inválida. Tente novamente.");
            }
        }
    }

    // ==== MENU ====
    private void showMenu() {
        System.out.println("\n===== Menu da Rede Social =====");
        System.out.println("1. Publicar texto");
        System.out.println("2. Seguir usuário");
        System.out.println("3. Enviar mensagem privada");
        System.out.println("4. Ver notificações");
        System.out.println("5. Ver timeline");
        System.out.println("6. Forçar atraso no relógio");
        System.out.println("7. Sair");
    }

    // ==== SIGNUP ====
    private boolean signupUser(Scanner scanner) {
        while (true) {
            JSONObject req = new JSONObject();
            req.put("action", "add_user");
            req.put("username", username);

            reqSocket.send(req.toString());
            String respStr = reqSocket.recvStr();
            JSONObject resp = new JSONObject(respStr);

            if (resp.getInt("ret") == 0) {
                userId = resp.getInt("id");
                notifyTopic = resp.getString("topic");
                notifSocket.subscribe(notifyTopic.getBytes());
                log("INFO", "Usuário '" + username + "' cadastrado com sucesso. ID: " + userId + ", tópico: " + notifyTopic);
                System.out.printf("Usuário '%s' cadastrado! ID=%d, tópico='%s'\n", username, userId, notifyTopic);
                return true;
            } else {
                System.out.print("Username já existe. Digite outro: ");
                username = scanner.next();
            }
        }
    }

    // ==== 1. POSTAR TEXTO ====
    private void postText(Scanner scanner) {
        System.out.print("Digite seu texto: ");
        String text = scanner.nextLine();

        long now = System.currentTimeMillis() / 1000L - forcedDelay;
        String timestamp = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss").format(new Date(now * 1000));

        JSONObject msg = new JSONObject();
        msg.put("action", "post_text");
        msg.put("username", username);
        msg.put("id", userId);
        msg.put("texto", text);
        msg.put("tempoEnvioMensagem", timestamp);

        reqSocket.send(msg.toString());
        reqSocket.recvStr();
        System.out.println("Texto publicado!");
        log("INFO", "Usuário '" + username + "' publicou texto: '" + text + "'");
    }

    // ==== 2. SEGUIR USUÁRIO ====
    private void followUser(Scanner scanner) {
        System.out.print("Digite o nome do usuário que deseja seguir: ");
        String toFollow = scanner.next();

        if (toFollow.equals(username)) {
            System.out.println("Você não pode seguir a si mesmo!");
            log("WARNING", "Tentativa de seguir a si mesmo.");
            scanner.nextLine(); // consume newline
            return;
        }

        JSONObject req = new JSONObject();
        req.put("action", "add_follower");
        req.put("id", userId);
        req.put("to_follow", toFollow);

        reqSocket.send(req.toString());
        String respStr = reqSocket.recvStr();
        JSONObject resp = new JSONObject(respStr);

        if (resp.getInt("ret") == 0) {
            System.out.println("Agora você está seguindo " + toFollow);
            log("INFO", "Usuário '" + username + "' seguiu '" + toFollow + "'");
        } else if (resp.getInt("ret") == 2) {
            System.out.println("Usuário não encontrado.");
            log("WARNING", "Usuário '" + toFollow + "' não encontrado para seguir por '" + username + "'");
        } else {
            System.out.println("Erro ao seguir usuário.");
            log("ERROR", "Erro ao seguir usuário.");
        }
        scanner.nextLine(); // consume newline
    }

    private void displayConversation(String sender, String recipient) {
        JSONObject req = new JSONObject();
        req.put("action", "get_private_messages");
        req.put("remetente", sender);
        req.put("destinatario", recipient);

        reqSocket.send(req.toString());
        String responseStr = reqSocket.recvStr();
        JSONObject response = new JSONObject(responseStr);

        // Espera: {"mensagens": [[mensagem, timestamp, remetente], ...]}
        System.out.println("\n📱 Conversa entre você e " + recipient);
        System.out.println("--------------------------------------------------");

        if (!response.has("mensagens")) {
            System.out.println("Nenhuma mensagem até agora.");
            return;
        }

        org.json.JSONArray messages = response.getJSONArray("mensagens");
        for (int i = 0; i < messages.length(); i++) {
            org.json.JSONArray item = messages.getJSONArray(i);
            if (item.length() != 3) continue;

            String message = item.getString(0);
            long ts = item.getLong(1);
            String msgSender = item.getString(2);

            String timeFormatted;
            try {
                java.text.SimpleDateFormat sdf = new java.text.SimpleDateFormat("HH:mm");
                sdf.setTimeZone(java.util.TimeZone.getDefault());
                timeFormatted = sdf.format(new java.util.Date(ts * 1000));
            } catch (Exception e) {
                timeFormatted = "??:??";
            }

            if (msgSender.equals(username)) {
                System.out.printf("%25s %s: %s  🕒%s\n", "", msgSender, message, timeFormatted);
            } else {
                System.out.printf("%s: %s  🕒%s\n", msgSender, message, timeFormatted);
            }
        }
    }

    // ==== 3. ENVIAR MENSAGEM PRIVADA ====
    private void sendPrivateMessage(Scanner scanner) {
        System.out.print("Digite o nome do usuário destino: ");
        String toUser = scanner.next();
        scanner.nextLine(); // consume newline

        if (toUser.equals(username)) {
            System.out.println("Você não pode enviar mensagem para si mesmo!");
            log("WARNING", "Tentativa de enviar mensagem privada para si mesmo.");
            return;
        }

        displayConversation(username, toUser); // Mostra o histórico antes do envio

        System.out.print("Digite a mensagem: ");
        String message = scanner.nextLine();

        long now = System.currentTimeMillis() / 1000L - forcedDelay;

        JSONObject req = new JSONObject();
        req.put("action", "add_private_message");
        req.put("remetente", username);
        req.put("destinatario", toUser);
        req.put("mensagem", message);
        req.put("timestamp", now); // Inteiro, epoch!

        reqSocket.send(req.toString());
        String respStr = reqSocket.recvStr();
        JSONObject resp = new JSONObject(respStr);

        if (resp.optInt("ret", 1) == 0) {
            System.out.println("Mensagem enviada com sucesso para " + toUser + "!");
            log("INFO", "Enviou mensagem privada para '" + toUser + "': " + message);
            displayConversation(username, toUser); // Mostra o histórico após o envio
        } else if (resp.optInt("ret", 1) == 2) {
            System.out.println("Usuário de destino não encontrado.");
            log("WARNING", "Tentativa de enviar mensagem privada para usuário inexistente: '" + toUser + "'");
        } else {
            System.out.println("Erro ao enviar mensagem privada.");
            log("ERROR", "Falha ao enviar mensagem privada para '" + toUser + "'");
        }
    }

    // ==== 4. NOTIFICAÇÕES ====
    private void listenNotifications() {
        while (true) {
            String msg = notifSocket.recvStr();
            notifLock.lock();
            try {
                if (notifications.size() < MAX_NOTIFICATIONS) {
                    notifications.add(msg);
                }
            } finally {
                notifLock.unlock();
            }
        }
    }

    private void showNotifications() {
        notifLock.lock();
        int total = notifications.size();
        if (total == 0) {
            System.out.println("Nenhuma nova notificação.");
        } else {
            for (int i = 0; i < total; i++) {
                System.out.printf("[%d] %s\n", i + 1, notifications.get(i));
            }
            notifications.clear();
        }
        notifLock.unlock();
        log("INFO", "Usuário '" + username + "' verificou notificações. Total: " + total);
    }

    // ==== 5. VER TIMELINE ====
    private void viewTimeline() {
        JSONObject req = new JSONObject();
        req.put("action", "get_timeline");

        reqSocket.send(req.toString());
        String resp = reqSocket.recvStr();

        log("INFO", "Usuário '" + username + "' visualizou a timeline.");
        JSONArray arr = new JSONArray(resp);
        System.out.println("--- Postagens Recebidas ---");
        for (int i = 0; i < arr.length(); i++) {
            JSONObject post = arr.getJSONObject(i);
            System.out.println("---------------------------");
            System.out.println("User: " + post.getString("username"));
            System.out.println("Texto: " + post.getString("texto"));
            System.out.println("Enviado em: " + post.getString("tempoEnvioMensagem"));
        }
    }

    // ==== 6. ATRASO FORÇADO ====
    private void setForcedDelay(Scanner scanner) {
        System.out.print("Digite o atraso em segundos (0 para nenhum): ");
        forcedDelay = scanner.nextInt();
        scanner.nextLine(); // consume newline
        System.out.printf("Atraso forçado configurado para %d segundos.\n", forcedDelay);
        log("INFO", "Usuário '" + username + "' configurou atraso forçado para " + forcedDelay + " segundos.");
    }

    // ==== LOGGER ====
    private void setupLogger() throws IOException {
        logger = Logger.getLogger(username);
        logger.setUseParentHandlers(false); // disable console output
        logFileHandler = new FileHandler(username + ".log", true);
        logFileHandler.setFormatter(new Formatter() {
            @Override
            public String format(LogRecord record) {
                String ts = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date(record.getMillis()));
                return ts + " - " + record.getLevel() + " - " + record.getMessage() + "\n";
            }
        });
        logger.addHandler(logFileHandler);
    }

    private void log(String level, String msg) {
        if (logger == null) return;
        Level lvl;
        switch (level) {
            case "INFO":
                lvl = Level.INFO;
                break;
            case "WARNING":
                lvl = Level.WARNING;
                break;
            case "ERROR":
                lvl = Level.SEVERE;
                break;
            default:
                lvl = Level.INFO;
        }
        logger.log(lvl, msg);
    }

    // ==== CLOSE ====
    private void close() {
        try {
            if (logFileHandler != null) logFileHandler.close();
        } catch (Exception ignored) {
        }
        if (reqSocket != null) reqSocket.close();
        if (notifSocket != null) notifSocket.close();
        if (context != null) context.close();
    }
}