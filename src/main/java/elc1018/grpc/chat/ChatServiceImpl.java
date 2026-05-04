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

    // Lista de streams ativos
    private final CopyOnWriteArrayList<StreamObserver<ChatMessage>> observersAtivos =
            new CopyOnWriteArrayList<>();

    // Conjunto deusernames já registrados
    private final Set<String> usuariosRegistrados =
            ConcurrentHashMap.newKeySet();

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

        boolean added = usuariosRegistrados.add(username);

        RegisterResponse resposta = RegisterResponse.newBuilder()
                .setSuccess(added)
                .setUsername(username)
                .build();

        System.out.println("Register chamado para: " + username + " -> " + (added ? "OK" : "DUPLICADO"));

        responseObserver.onNext(resposta);
        responseObserver.onCompleted();
    }

    @Override
    public void receiveMessages(User request, StreamObserver<ChatMessage> responseObserver) {
        String username = request.getUsername();

        observersAtivos.add(responseObserver);

        io.grpc.Context.current().addListener(context -> {
            observersAtivos.remove(responseObserver);
            broadcast(criarMensagemSistema(username + " saiu da sala"));
            System.out.println(username + " desconectou.");
        }, java.util.concurrent.Executors.newSingleThreadExecutor());

        broadcast(criarMensagemSistema(username + " entrou na sala"));

        System.out.println(username + " abriu stream de recebimento.");
    }

    @Override
    public void sendMessage(ChatMessage request, StreamObserver<Ack> responseObserver) {

        broadcast(request);

        Ack ack = Ack.newBuilder().setSuccess(true).build();
        responseObserver.onNext(ack);

        System.out.println("Mensagem recebida de " + request.getFrom() + ": " + request.getContent());

        responseObserver.onCompleted();
    }

    private void broadcast(ChatMessage mensagem) {
        for (StreamObserver<ChatMessage> observer : observersAtivos) {
            try {
                observer.onNext(mensagem);
            } catch (Exception e) {
                observersAtivos.remove(observer);
            }
        }
    }

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