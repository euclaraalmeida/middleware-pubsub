# Cliente de publicação de temperaturas

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
