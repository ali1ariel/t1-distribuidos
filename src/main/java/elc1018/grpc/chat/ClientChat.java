package elc1018.grpc.chat;

import com.google.protobuf.Timestamp;
import elc1018.grpc.chat.protos.Ack;
import elc1018.grpc.chat.protos.ChatMessage;
import elc1018.grpc.chat.protos.ChatServiceGrpc;
import elc1018.grpc.chat.protos.RegisterResponse;
import elc1018.grpc.chat.protos.User;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.stub.StreamObserver;

import java.time.Instant;
import java.util.Scanner;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

public class ClientChat {

    private final ManagedChannel canal;
    private final ChatServiceGrpc.ChatServiceBlockingStub stubBlocking;
    private final ChatServiceGrpc.ChatServiceStub stubAsync;
    private String username;

    public ClientChat(String host, int porta) {
        canal = ManagedChannelBuilder
                .forAddress(host, porta)
                .usePlaintext()
                .build();

        // usado para Register e SendMessage (unary)
        stubBlocking = ChatServiceGrpc.newBlockingStub(canal);

        // usado para ReceiveMessages (server streaming)
        stubAsync = ChatServiceGrpc.newStub(canal);
    }

    public boolean registrar(String username) {
        User user = User.newBuilder()
                .setUsername(username)
                .build();

        RegisterResponse resposta = stubBlocking.register(user);

        if (resposta.getSuccess()) {
            this.username = username;
            System.out.println("Registrado com sucesso como: " + username);
            return true;
        } else {
            System.out.println("Erro: nome de usuário '" + username + "' já está em uso. Tente outro.");
            return false;
        }
    }

    public void iniciarRecebimento(CountDownLatch latch) {
        User user = User.newBuilder()
                .setUsername(username)
                .build();

        stubAsync.receiveMessages(user, new StreamObserver<ChatMessage>() {

            @Override
            public void onNext(ChatMessage mensagem) {
                String hora = formatarTimestamp(mensagem.getTimestamp());
                System.out.println("\n[" + hora + "] " + mensagem.getFrom() + ": " + mensagem.getContent());
                System.out.print("> ");
            }

            @Override
            public void onError(Throwable t) {
                System.out.println("\nConexão com o servidor encerrada: " + t.getMessage());
                latch.countDown();
            }

            @Override
            public void onCompleted() {
                System.out.println("\nServidor encerrou a conexão.");
                latch.countDown();
            }
        });
    }

    public void enviarMensagem(String conteudo) {
        Instant agora = Instant.now();

        ChatMessage mensagem = ChatMessage.newBuilder()
                .setFrom(username)
                .setContent(conteudo)
                .setTimestamp(Timestamp.newBuilder()
                        .setSeconds(agora.getEpochSecond())
                        .setNanos(agora.getNano())
                        .build())
                .build();

        Ack ack = stubBlocking.sendMessage(mensagem);

        if (!ack.getSuccess()) {
            System.out.println("Aviso: servidor não confirmou o recebimento da mensagem.");
        }
    }

    public void encerrar() throws InterruptedException {
        canal.shutdown().awaitTermination(5, TimeUnit.SECONDS);
    }

    private String formatarTimestamp(Timestamp ts) {
        if (ts == null || (ts.getSeconds() == 0 && ts.getNanos() == 0)) {
            return "--:--:--";
        }
        Instant instant = Instant.ofEpochSecond(ts.getSeconds(), ts.getNanos());
        java.time.LocalTime hora = java.time.LocalTime.ofInstant(instant, java.time.ZoneId.systemDefault());
        return String.format("%02d:%02d:%02d", hora.getHour(), hora.getMinute(), hora.getSecond());
    }

    public static void main(String[] args) throws InterruptedException {
        String host = "localhost";
        int porta = 50051;

        Scanner scanner = new Scanner(System.in);
        ClientChat cliente = new ClientChat(host, porta);

        boolean registrado = false;
        while (!registrado) {
            System.out.print("Digite seu nome de usuário: ");
            String username = scanner.nextLine().trim();

            if (username.isEmpty()) {
                System.out.println("Nome não pode ser vazio.");
                continue;
            }

            registrado = cliente.registrar(username);
        }

        CountDownLatch latch = new CountDownLatch(1);

        cliente.iniciarRecebimento(latch);

        System.out.println("Conectado! Digite suas mensagens abaixo (ou 'sair' para encerrar):");
        System.out.print("> ");

        while (latch.getCount() > 0) {
            if (scanner.hasNextLine()) {
                String linha = scanner.nextLine().trim();

                if (linha.equalsIgnoreCase("sair")) {
                    System.out.println("Saindo...");
                    break;
                }

                if (!linha.isEmpty()) {
                    cliente.enviarMensagem(linha);
                }

                if (latch.getCount() > 0) {
                    System.out.print("> ");
                }
            }
        }

        cliente.encerrar();
        scanner.close();
        System.out.println("Conexão encerrada.");
    }
}