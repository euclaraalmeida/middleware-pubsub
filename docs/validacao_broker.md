# Validação de capacidade e recuperação

Esta etapa complementa os cenários da Clara sem editar seus clientes, seu
avaliador ou os resultados publicados por ela. Usa Python 3.11 e a biblioteca
padrão. A execução cria portas livres e bancos temporários.

## Testes de comportamento

Na raiz do repositório:

```powershell
python -B -m unittest discover -s testes -v
```

A suíte cobre publicação, destinatários fixados na aceitação, descartes,
deduplicação, ACK individual, reentrega, reconexão e operações com pendências.
Os testes desta etapa acrescentam:

- Disputa entre publicadores sem ultrapassar a capacidade.
- Limite padrão de 10.000 registros após reabrir o banco.
- Espera sem ACK e retomada somente após a última confirmação.
- Reenvios com o mesmo UUID durante a espera.
- Publicação e confirmação na mesma conexão, sem bloqueio mútuo.
- Encerramento e desconexão durante a espera, com liberação de identidade.
- Reinício com banco cheio e retransmissão do pedido ainda não confirmado.
- Publicador real repetindo a leitura após supressão do primeiro ACK.
- Processo do broker encerrado à força após um ACK de sucesso e recuperação
  da entrega com o assinante real, usando o mesmo SQLite.

Nos testes rápidos, alguns intervalos são reduzidos explicitamente. O teste
de 10.000 registros prepara o banco em lote para conferir a fronteira. A
validação abaixo também preenche esse limite com publicações TCP reais.

## Integração e saturação pela rede

```powershell
python -B -m testes.avaliar_broker
```

Não inicie outro broker para esse comando. Ele inicia e encerra os seus
próprios servidores. As etapas são:

1. Rodar os seis cenários do avaliador da Clara com os parâmetros dos ensaios
   iniciais, contra o broker com capacidade implementada.
2. Executar uma publicação com o primeiro ACK suprimido no envio do broker.
   O cliente real espera cinco segundos e repete o mesmo UUID. A primeira
   entrega já pode ter sido concluída; o resultado persistido impede recriá-la.
3. Cadastrar um inscrito offline e publicar 10.000 leituras pela biblioteca
   real, recebendo os ACKs correspondentes.
4. Enviar outra leitura e verificar ausência de ACK enquanto o banco está cheio.
   Um comando posterior na mesma conexão confirma que o pedido já foi lido.
5. Reconectar o inscrito, confirmar uma leitura e verificar que o pedido em
   espera é aceito. A contagem volta a 10.000, sem ultrapassar o limite.
6. Reiniciar o broker e conferir as 10.000 pendências e a deduplicação do UUID
   que acabou de ser aceito.

O modo de falha usa `unittest.mock` apenas no processo desse verificador.
Não há parâmetro de perda de ACK no servidor normal nem alteração no protocolo.

Cada execução salva seus próprios CSVs em `resultados/broker/validacao-<id>/`.
`manifesto.txt` identifica ambiente, horário UTC e SHA-256 das fontes usadas.
Os CSVs do avaliador mantêm o formato original. `saturacao.csv` registra a
fronteira de capacidade e `verificacoes.txt` só é escrito após todas as
verificações passarem. Os bancos temporários são removidos ao final.

## Registro de 21/11/2026

A execução `validacao-a23a3486314543788dbccde792daff5d`, em
[`resultados/broker`](../resultados/broker/validacao-a23a3486314543788dbccde792daff5d/),
terminou com as sete execuções do avaliador sem erros ou entregas faltantes.
A perda de ACK produziu uma retransmissão e uma única chegada. A saturação
preencheu as 10.000 vagas por TCP, aguardou sem ACK e aceitou uma leitura
após liberar uma vaga. O reinício preservou 10.000 pendências. A etapa de
saturação levou aproximadamente quatro minutos nessa máquina; reserve alguns
minutos para repetir o comando completo. A suíte terminou com 77 testes
aprovados. O manifesto corresponde às fontes executadas.

## Como observar um ensaio manual

```powershell
python -m broker --banco dados/ensaio.db --capacidade 10
```

Em outro terminal:

```powershell
python -m broker.estado --banco dados/ensaio.db
```

O comando consulta o estado persistido naquele instante. Não mostra filas
RAM dos clientes nem pedidos ainda não aceitos no broker. Para acompanhar a
evolução, repita a consulta durante o ensaio. Use um arquivo de banco novo
para cada ensaio independente e o mesmo arquivo quando testar recuperação.

O consumidor lento do avaliador continua enviando ACK ao receber na biblioteca.
Para reter pendências no broker, o verificador de saturação mantém o inscrito
offline e depois confirma apenas uma entrega.

## Limites da evidência

Os testes de reinício cobrem encerramento normal e término forçado do processo,
sem simular queda de energia ou falha física do armazenamento. Os cenários
locais validam integração e coleta; uma execução de cada não caracteriza o
desempenho geral. Os tempos incluem SQLite, TCP, agendamento e instrumentação.
Repetições, cargas mais variadas e gráficos permanecem na avaliação da Clara.
