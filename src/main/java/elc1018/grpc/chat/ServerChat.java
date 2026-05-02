package elc1018.grpc.chat;

import io.grpc.protobuf.services.ProtoReflectionService;

import io.grpc.Server;
import io.grpc.ServerBuilder;

import java.io.IOException;

public class ServerChat {

    private Server server;
    private static final int PORTA = 50051;

    public void iniciar() throws IOException {
        server = ServerBuilder
                .forPort(PORTA)
                .addService(new ChatServiceImpl())
                .addService(ProtoReflectionService.newInstance())
                .build()
                .start();

        System.out.println("Servidor iniciado na porta " + PORTA);

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            System.out.println("Encerrando servidor...");
            ServerChat.this.encerrar();
        }));
    }

    public void encerrar() {
        if (server != null) {
            server.shutdown();
        }
    }

    public void aguardarEncerramento() throws InterruptedException {
        if (server != null) {
            server.awaitTermination();
        }
    }

    public static void main(String[] args) throws IOException, InterruptedException {
        ServerChat servidor = new ServerChat();
        servidor.iniciar();
        servidor.aguardarEncerramento();
    }
}