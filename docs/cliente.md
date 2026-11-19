# Cliente Publish/Subscribe de temperaturas

O cliente de quarta-feira está em `client/publicador.py`. Use um
`Publicador` por conexão, chamando seus métodos em sequência. O sensor informa
o tópico e a temperatura. A biblioteca mantém a fila, cria os UUIDs e usa
`shared.protocolo` para escrever e ler os quadros TCP.

## Uso em Python

```python
from client.publicador import Publicador

cliente = Publicador("127.0.0.1", 5000, cliente_id="sensor-agua-01")
try:
    cliente.conectar()
    uuid = cliente.enfileirar("agua/temperatura", 24.5)
    print(f"{uuid} enfileirada")
    resultado = cliente.publicar_proxima()
    print(f"{uuid}: {resultado.name}, pendentes na fila: {cliente.pendentes}")
finally:
    cliente.fechar()
```

`conectar()` é opcional antes da primeira publicação, pois
`publicar_proxima()` abre a conexão quando necessário. A mesma conexão é
reutilizada para as próximas leituras. Quando `cliente_id` é fornecido, a
biblioteca envia `IDENTIFICAR` e aguarda `RESPOSTA` com o mesmo ID de
solicitação. Uma recusa gera `IdentificacaoRecusada`.

O broker atual exige identificação antes de processar comandos. O quadro
`PUBLICAR` carrega UUID, tópico e temperatura. Seu formato permite que o
identificador seja omitido na configuração do cliente, para uso com um
broker que aceite produtores sem identificação. O exemplo gera um
`cliente_id` para a execução por padrão, para atender ao broker atual.
Identidade estável entre execuções é relevante para inscritos com
entregas pendentes.

## Fila, UUID e ACK

1. `enfileirar()` cria um UUID e adiciona a leitura ao fim da fila em RAM.
   Com 1.000 leituras pendentes, uma nova inclusão gera `FilaCheia` e
   preserva as leituras anteriores.
2. `publicar_proxima()` envia apenas a primeira leitura da fila. O cliente
   aguarda o ACK por até cinco segundos. Se o prazo expirar, repete o envio
   com o mesmo UUID e um novo ID de solicitação. A chamada fica ocupada
   enquanto aguarda ou repete o envio.
3. O ACK deve ter a operação `ACK_PUBLICACAO`, o ID da tentativa atual e
   o UUID da leitura. Respostas atrasadas com outro ID são ignoradas. Um
   UUID incorreto na resposta à tentativa atual gera `ErroProtocolo`.
4. `OK` e `DESCARTADA_SEM_INSCRITOS` retiram a leitura da fila. Qualquer
   outro resultado é devolvido como recusa e mantém a leitura na frente da
   fila. A aplicação pode investigar a recusa antes de tentar novamente.
5. Se a conexão cair, o cliente propaga o erro e preserva a fila no objeto
   em RAM. Uma nova chamada a `publicar_proxima()` no mesmo objeto pode
   reconectar e tentar a mesma leitura. A fila se perde ao encerrar o processo.

## Sensor simulado

Da raiz do projeto, com o broker em execução:

```powershell
python -m exemplos.sensor_agua --host 127.0.0.1 --porta 5000 --topico agua/temperatura
```

O sensor mostra o UUID, a espera de ACK, o reenvio por timeout e o resultado.
Use `--cliente-id` para escolher a identificação. `--sem-identificacao`
serve para um broker que permita produtores sem identificação.

## Verificação e integração

`python -B -m unittest discover -s testes -v` executa os testes do projeto.
Os testes do publicador substituem chamadas locais do socket para exercitar
fila, ordem, timeout e ACKs, sem subir um servidor TCP simulado. O teste
com o broker da Mariana cobre identificação e reutilização da conexão.
A criação de tópicos é exercitada pelos testes existentes do broker.

O broker atual recusa a publicação com `REQUISICAO_INVALIDA`. Falta
validar o sensor de ponta a ponta quando o broker implementar seleção dos
inscritos, persistência da mensagem e dos destinatários, deduplicação por
UUID, `OK` após gravação e `DESCARTADA_SEM_INSCRITOS` sem destinatários.
Nesse momento, os testes de integração poderão cobrir publicação aceita,
descarte e reenvio após perda do ACK sem gravação duplicada.

## Cliente inscrito

`client/assinante.py` oferece a classe `Assinante`. Ela usa o mesmo
transporte de `Publicador`, em `client/conexao.py`. Configure um
`cliente_id` fixo para cada aplicação inscrita e mantenha esse valor
mesmo ao reiniciar o processo. Use IDs diferentes para aplicações
conectadas simultaneamente.

```python
from client.assinante import Assinante
from shared.protocolo import Resultado

cliente = Assinante("127.0.0.1", 5000, cliente_id="painel-agua-01")
try:
    cliente.conectar()
    resposta = cliente.criar_topico("agua/temperatura")
    if resposta.campos["resultado"] not in (Resultado.OK, Resultado.TOPICO_JA_EXISTENTE):
        raise RuntimeError(resposta.campos["resultado"].name)
    resposta = cliente.inscrever("agua/temperatura")
    if resposta.campos["resultado"] == Resultado.OK:
        print(cliente.receber().campos)
    else:
        print(resposta.campos["resultado"].name)
finally:
    cliente.fechar()
```

Os quatro métodos de gerenciamento, `criar_topico()`, `excluir_topico()`,
`inscrever()` e `cancelar_inscricao()`, retornam a `Mensagem` de
`RESPOSTA` correspondente, preservando o código de resultado e o texto
do broker. Se o comando perder a conexão ou expirar, a aplicação recebe
o erro. O cliente evita repetir o comando automaticamente porque o broker
pode já tê-lo executado.

A recepção roda em segundo plano. Uma trava coordena o acesso ao socket
com os comandos. Enquanto aguarda uma resposta, o próprio comando também
encaminha eventuais entregas intercaladas para a fila.

1. O acumulador reconstrói um quadro `ENTREGAR` com os bytes recebidos do TCP.
2. O assinante coloca a mensagem na fila RAM e registra o UUID em memória.
3. O assinante envia `ACK_ENTREGA` com ID de solicitação zero, UUID e
   `cliente_id`. Uma retransmissão de UUID já aceito recebe outro ACK,
   sem uma nova entrada na fila.
4. A aplicação chama `receber()` para retirar a próxima mensagem. O ACK
   confirma a aceitação na fila, independentemente de quando a aplicação
   consome ou termina de processar a leitura. Use `receber(timeout=2)`
   para esperar até dois segundos e receber `queue.Empty` se a fila
   continuar vazia.

Após perder uma conexão estabelecida, o receptor espera cinco segundos
antes de cada nova tentativa. Ele recria o acumulador e se identifica com
o mesmo ID. A fila e os UUIDs aceitos sobrevivem à reconexão no mesmo
objeto em execução. Ao reiniciar o processo, ambos começam vazios.
O mesmo ID permite ao broker recuperar inscrições e entregas pendentes
quando esse fluxo estiver disponível. Chame `fechar()` ao encerrar o
cliente para parar as tentativas.

Com o tópico já criado e o broker oferecendo inscrições e entregas,
execute da raiz do projeto:

```powershell
python -m exemplos.painel_agua --cliente-id painel-agua-01 --topico agua/temperatura
```

Com a versão atual do broker, a integração verifica identificação e
criação de tópicos. Ainda falta verificar com o broker real inscrição,
exclusão, cancelamento, resultados de operações com pendências,
encaminhamento, registro de ACK e reentrega após reconexão. Os testes
isolados cobrem os quadros e a lógica do cliente substituindo chamadas
locais do socket.
