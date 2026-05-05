# Chat gRPC — ELC1018 Sistemas Distribuídos

Aplicação de chat distribuído com suporte a múltiplos usuários em uma única sala, utilizando Java, gRPC e Protocol Buffers. Desenvolvido utilizando o IntelliJ como editor e compilado utilizando as ferramentas do editor.

## Arquitetura

```
ClientChat  ──┐
ClientChat  ──┤── gRPC (porta 50051) ──► ServerChat
ClientChat  ──┘                              │
                                        ChatServiceImpl
```

### Processos

| Processo | Classe | Descrição |
|---|---|---|
| Servidor | `ServerChat` | Gerencia a sala única e faz broadcast das mensagens |
| Cliente | `ClientChat` | Registra usuário, envia e recebe mensagens em tempo real |

## Como compilar

```bash
mvn clean package
```

O build gera um fat-jar em `target/chat-1.0-SNAPSHOT.jar` via `maven-shade-plugin`.

## Como executar

### Servidor

```bash
java -cp target/chat-1.0-SNAPSHOT.jar elc1018.grpc.chat.ServerChat
```

O servidor sobe na porta **50051**.

### Cliente (em outro terminal)

```bash
java -cp target/chat-1.0-SNAPSHOT.jar elc1018.grpc.chat.ClientChat
```

Cada instância do cliente é um usuário diferente. Inicie quantas quiser.

### Interação no cliente

```
Digite seu nome de usuário: alice
Registrado com sucesso como: alice
Conectado! Digite suas mensagens abaixo (ou 'sair' para encerrar):
> Olá a todos!
> sair
```

## Requisitos funcionais implementados

| ID | Requisito | Implementação |
|---|---|---|
| RFA01 | Registro com username único; nomes duplicados rejeitados | `ChatServiceImpl.register()` — usa `ConcurrentHashMap` para garantir unicidade |
| RFA02 | Sala única criada pelo servidor; aceita múltiplos usuários | `ChatServiceImpl` mantém lista única de observers ativos |
| RFA03 | `SendMessage` encaminha mensagem a todos da sala | `broadcast()` itera sobre todos os streams ativos |
| RFA04 | `ChatMessage` contém `from`, `content` e `timestamp` | Preenchidos pelo cliente em `enviarMensagem()` |
| RFA05 | `ReceiveMessages` mantém stream ativo por usuário | Stream adicionado em `receiveMessages()` e mantido aberto enquanto o cliente estiver conectado |
| RFA06 | Ordem das mensagens preservada por remetente | gRPC garante entrega serial por stream; `CopyOnWriteArrayList` mantém ordem de inserção |
| RFA07 | Notificação a todos quando usuário entra ou sai | Mensagem de sistema (`SERVER`) enviada via `broadcast()` no join e no cancelamento de contexto |

## Modelos de comunicação utilizados

- **Unary**: `Register` e `SendMessage` — requisição única com resposta única
- **Server Streaming**: `ReceiveMessages` — o servidor empurra mensagens continuamente ao cliente
