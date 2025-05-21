
# Sistema Distribuído com Replicação + Sincronização de Relógios (Berkeley + Bullying)

## Requisitos

- Python 3.7+
- Biblioteca zmq instalada:
  pip install pyzmq

---

## Como executar

Abra **3 terminais separados** (um para cada servidor) e execute:

Terminal 1:
> python main_server.py 1

Terminal 2:
> python main_server.py 2

Terminal 3:
> python main_server.py 3

---

## O que acontece:

1. Cada servidor começa com um relógio local.
2. Após 5 segundos, inicia-se uma eleição via Bullying.
3. O coordenador executa o algoritmo de Berkeley para sincronizar os relógios dos outros.
4. Você pode digitar postagens e elas serão replicadas automaticamente entre os 3 servidores.
5. As réplicas aparecem com o `[RECEIVED REPLICATION]` no terminal dos outros servidores.

---

## Teste a resiliência

- Feche um servidor e envie posts de outro — o sistema continuará funcionando.
- Reabra o servidor e ele poderá participar da eleição na próxima sincronização.

---

## Arquivos

- `main_server.py`: inicializa um servidor com ID e peers.
- `replication.py`: replicação de posts via PUSH/PULL.
- `clock_sync.py`: eleição + sincronização de relógio.
- `common.py`: funções utilitárias ZMQ.

---

Bom estudo!
