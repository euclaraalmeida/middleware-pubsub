# Protocolo binário Publish/Subscribe

Cada quadro começa com um cabeçalho de 9 bytes. Os campos numéricos usam
big-endian e são empacotados com `struct`, sem bytes de preenchimento.

| Offset | Campo | Tamanho | Formato |
| --- | --- | --- | --- |
| 0 | Tamanho do conteúdo | 4 bytes | `>I`, inteiro sem sinal |
| 4 | Operação | 1 byte | `>B`, inteiro sem sinal |
| 5 | ID da solicitação | 4 bytes | `>I`, inteiro sem sinal |
| 9 | Campos da operação | Variável | Conforme a tabela de operações |

O tamanho do conteúdo conta todos os bytes após o prefixo de 4 bytes,
incluindo a operação e o ID. O limite para o conteúdo é de 5 a 65.562 bytes,
com restrições adicionais conforme a operação. O maior quadro tem 65.566 bytes.

## Tipos de campo

| Tipo | Representação |
| --- | --- |
| Texto | Comprimento em bytes (`>H`, 2 bytes), seguido pelos bytes UTF-8. O limite é de 65.535 bytes por campo. Tópicos e IDs de cliente devem conter pelo menos um byte. |
| UUID | 16 bytes na ordem de `uuid.UUID.bytes` do Python. O emissor reutiliza o mesmo UUID se retransmitir a publicação. |
| Temperatura | 4 bytes em IEEE 754 float32 (`>f`). Aceita valores finitos representáveis após arredondamento para float32. |
| Resultado | 1 byte (`>B`), com um dos códigos da tabela de resultados. |

## Operações

Os campos listados aparecem na ordem indicada, logo após o cabeçalho.
O símbolo `n` representa o número de bytes UTF-8 do campo de texto de cada linha.

| Código | Operação | Campos em ordem e tamanhos |
| --- | --- | --- |
| 1 | IDENTIFICAR | `cliente_id` (2 + n) |
| 2 | CRIAR_TOPICO | `topico` (2 + n) |
| 3 | EXCLUIR_TOPICO | `topico` (2 + n) |
| 4 | INSCREVER | `topico` (2 + n) |
| 5 | CANCELAR_INSCRICAO | `topico` (2 + n) |
| 6 | PUBLICAR | `uuid` (16), `topico` (2 + n), `temperatura` (4) |
| 7 | ENTREGAR | `uuid` (16), `topico` (2 + n), `temperatura` (4) |
| 8 | ACK_PUBLICACAO | `uuid` (16), `resultado` (1) |
| 9 | ACK_ENTREGA | `uuid` (16), `cliente_id` (2 + n) |
| 10 | RESPOSTA | `resultado` (1), `mensagem` (2 + n) |

Em RESPOSTA, uma mensagem explicativa ausente é representada por comprimento
zero (`0000`) e nenhum byte de texto. O campo de comprimento está sempre presente.

Os comandos 1 a 5 recebem RESPOSTA e PUBLICAR recebe ACK_PUBLICACAO. Cada
resposta usa o mesmo ID de solicitação do comando correspondente. Os IDs
desses pares vão de 1 a 4.294.967.295. ENTREGAR e ACK_ENTREGA usam ID zero.
ACK_ENTREGA referencia o UUID recebido em ENTREGAR e encerra a confirmação
da entrega.

## Códigos de resultado

| Código | Resultado |
| --- | --- |
| 0 | OK |
| 1 | DESCARTADA_SEM_INSCRITOS |
| 2 | TOPICO_COM_PENDENCIAS |
| 3 | INSCRICAO_COM_PENDENCIAS |
| 4 | TOPICO_INEXISTENTE |
| 5 | TOPICO_JA_EXISTENTE |
| 6 | CLIENTE_ID_EM_USO |
| 7 | REQUISICAO_INVALIDA |

## Exemplos em hexadecimal

Cada linha contém um quadro completo. Os espaços delimitam campos.
Os exemplos usam o cliente `s1`, o tópico `é` (2 bytes UTF-8), a temperatura
25,5 e o UUID `00112233-4455-6677-8899-aabbccddeeff`.

| Operação | Quadro |
| --- | --- |
| IDENTIFICAR | `00000009 01 00000001 0002 7331` |
| CRIAR_TOPICO | `00000009 02 00000001 0002 c3a9` |
| EXCLUIR_TOPICO | `00000009 03 00000001 0002 c3a9` |
| INSCREVER | `00000009 04 00000001 0002 c3a9` |
| CANCELAR_INSCRICAO | `00000009 05 00000001 0002 c3a9` |
| PUBLICAR | `0000001d 06 00000002 00112233445566778899aabbccddeeff 0002 c3a9 41cc0000` |
| ENTREGAR | `0000001d 07 00000000 00112233445566778899aabbccddeeff 0002 c3a9 41cc0000` |
| ACK_PUBLICACAO | `00000016 08 00000002 00112233445566778899aabbccddeeff 00` |
| ACK_ENTREGA | `00000019 09 00000000 00112233445566778899aabbccddeeff 0002 7331` |
| RESPOSTA | `0000000c 0a 00000001 04 0004 6ec3a36f` |

RESPOSTA no exemplo contém TOPICO_INEXISTENTE e o texto `não`.
Uma RESPOSTA OK sem explicação seria `00000008 0a 00000001 00 0000`.

## Delimitação no fluxo TCP

O TCP entrega um fluxo de bytes. Uma leitura pode trazer parte do prefixo,
parte do conteúdo ou vários quadros juntos. O receptor deve acumular bytes
até ter os 4 bytes do prefixo, ler e validar o tamanho declarado e continuar
acumulando até ter exatamente aquele número de bytes de conteúdo após o
prefixo. Então pode passar o quadro completo para `decodificar` e manter os
bytes que sobrarem no buffer para o próximo quadro. Se a conexão terminar
com um quadro parcial no buffer, o quadro está truncado.

## Uso em Python

```python
from shared.protocolo import Operacao, codificar, decodificar

quadro = codificar(Operacao.INSCREVER, 7, topico="sala")
mensagem = decodificar(quadro)
assert mensagem.id_solicitacao == 7
assert mensagem.campos["topico"] == "sala"
```

`codificar` recebe os campos pelo nome, com UUIDs como objetos `uuid.UUID`.
`decodificar` recebe um quadro completo do tipo `bytes` e devolve uma
`Mensagem` com a operação, o ID e os campos. As duas funções levantam
`ErroProtocolo` ao detectar campos inválidos ou quadros malformados.
