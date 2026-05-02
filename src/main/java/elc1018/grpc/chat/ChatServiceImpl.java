package elc1018.grpc.chat;

import com.google.protobuf.Timestamp;
import elc1018.grpc.chat.protos.ChatMessage;
import elc1018.grpc.chat.protos.ChatServiceGrpc;
import elc1018.grpc.chat.protos.RegisterResponse;
import elc1018.grpc.chat.protos.User;
import elc1018.grpc.chat.protos.Ack;
import io.grpc.stub.StreamObserver;

import java.time.Instant;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;

public class ChatServiceImpl extends ChatServiceGrpc.ChatServiceImplBase {

    // Lista thread-safe de streams ativos — um por cliente conectado
    private final CopyOnWriteArrayList<StreamObserver<ChatMessage>> observersAtivos =
            new CopyOnWriteArrayList<>();

    // Conjunto thread-safe de usernames já registrados
    private final Set<String> usuariosRegistrados =
            ConcurrentHashMap.newKeySet();

    // -------------------------------------------------------------------------
    // RFA01 — Registro de usuário único
    // -------------------------------------------------------------------------
    @Override
    public void register(User request, StreamObserver<RegisterResponse> responseObserver) {
        String username = request.getUsername();
        if (username == null) {
            RegisterResponse resposta = RegisterResponse.newBuilder()
                    .setSuccess(false)
                    .setUsername("")
                    .build();
            responseObserver.onNext(resposta);
            responseObserver.onCompleted();
            return;
        }

        // Tenta adicionar ao conjunto; se já existir, add() retorna false
        boolean added = usuariosRegistrados.add(username);

        RegisterResponse resposta = RegisterResponse.newBuilder()
                .setSuccess(added)
                .setUsername(username)
                .build();

        System.out.println("Register chamado para: " + username + " -> " + (added ? "OK" : "DUPLICADO"));

        responseObserver.onNext(resposta);
        responseObserver.onCompleted();
    }

    // -------------------------------------------------------------------------
    // RFA05 — Stream de recebimento de mensagens
    // -------------------------------------------------------------------------
    @Override
    public void receiveMessages(User request, StreamObserver<ChatMessage> responseObserver) {
        String username = request.getUsername();

        // TODO: adicionar o observer à lista de ativos
        // Dica: observersAtivos.add(responseObserver)

        // TODO (RFA07): emitir notificação de entrada para todos
        // Dica: criarMensagemSistema(username + " entrou na sala")
        //       e chamar broadcast(mensagem)

        System.out.println(username + " abriu stream de recebimento.");

        // Observação: este método NÃO chama onCompleted() aqui —
        // o stream fica aberto até o cliente desconectar.
        // A desconexão é detectada via onError() ou onCompleted() do observer.
    }

    // -------------------------------------------------------------------------
    // RFA03 — Envio e broadcast de mensagens
    // -------------------------------------------------------------------------
    @Override
    public void sendMessage(ChatMessage request, StreamObserver<Ack> responseObserver) {

        // TODO: fazer broadcast da mensagem para todos os observers ativos
        // Dica: broadcast(request)

        // TODO: responder com Ack(success = true)
        // Dica: Ack.newBuilder().setSuccess(true).build()

        System.out.println("Mensagem recebida de " + request.getFrom() + ": " + request.getContent());

        responseObserver.onCompleted();
    }

    // -------------------------------------------------------------------------
    // Método auxiliar — Broadcast para todos os clientes conectados
    // -------------------------------------------------------------------------
    private void broadcast(ChatMessage mensagem) {
        // TODO: iterar sobre observersAtivos e chamar observer.onNext(mensagem)
        // Dica: use um for-each com try/catch para remover observers com erro
        //
        // for (StreamObserver<ChatMessage> observer : observersAtivos) {
        //     try {
        //         observer.onNext(mensagem);
        //     } catch (Exception e) {
        //         observersAtivos.remove(observer);
        //     }
        // }
    }

    // -------------------------------------------------------------------------
    // Método auxiliar — Cria uma mensagem de sistema (entrada/saída)
    // -------------------------------------------------------------------------
    private ChatMessage criarMensagemSistema(String conteudo) {
        Instant agora = Instant.now();
        return ChatMessage.newBuilder()
                .setFrom("SERVER")
                .setContent(conteudo)
                .setTimestamp(Timestamp.newBuilder()
                        .setSeconds(agora.getEpochSecond())
                        .setNanos(agora.getNano())
                        .build())
                .build();
    }
}