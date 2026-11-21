# Andamento em 21/11/2026

Conferência baseada no cronograma de implementação e avaliação de Clara e
Mariana e nos arquivos disponíveis no repositório. Implementação local pronta
precisa ser publicada e incorporada à `main` para disponibilizar a integração
à outra integrante.

| Dia | Clara | Mariana |
| --- | --- | --- |
| Terça, 17/11 | Protocolo binário, codificação, reconstrução de quadros e testes publicados. | TCP concorrente, identificação, SQLite e testes publicados. |
| Quarta, 18/11 | Publicador, UUID, fila RAM, ACK e reenvio publicados. | Publicação atômica, descarte e deduplicação publicados. |
| Quinta, 19/11 | Assinante, comandos, ACK na RAM e reconexão publicados. | Inscrições, entregas, ACK individual e recuperação publicados. |
| Sexta, 20/11 | Seis cenários, instrumentação, CSVs e instruções publicados. | Capacidade, espera sem ACK, liberação de vagas, verificações e documentação implementadas nesta entrega local. |
| Sábado, 21/11 | Faltam repetições com cargas variadas, gráficos e análise final. | Relatório técnico redigido, validações complementares disponíveis; falta incorporar os gráficos e a análise final e revisar com Clara. |

## Entrega técnica da Mariana

- Limite padrão de 10.000 leituras pendentes e ajuste por `--capacidade`.
- Retomada após ACK sem prender o recebimento de comandos na conexão.
- Identificação duplicada, limpeza de conexões, encerramento e SQLite concorrente.
- Consulta direta de pendências por `python -m broker.estado`.
- Testes de capacidade, reenvio com ACK perdido, reinício e término forçado.
- Verificador que usa o avaliador da Clara e preenche 10.000 vagas por TCP.
- Relatório com arquitetura, decisões, trade-offs, evidências e limitações.

Os registros de validação local ficam em `resultados/broker/`. Os resultados
anteriores de Clara permanecem nos diretórios originais. Não há alteração da
biblioteca cliente, do protocolo nem do avaliador de Clara nesta etapa.

## O que ainda depende de Clara

- Repetir cenários com diferentes números de sensores, inscritos e rajadas,
  guardando parâmetros, ambiente e revisão do broker atualizado.
- Gerar os gráficos de latência, vazão, filas e confirmações, com unidades
  e interpretação dos resultados.
- Concluir a seção de método e avaliação de desempenho. Uma rodada local
  de cada cenário não permite generalizar o desempenho do sistema.
- Atualizar o parágrafo final de `docs/cliente.md`, que ainda descreve a
  integração como se inscrições e entregas não tivessem sido verificadas.

A perda controlada do ACK de publicação já está automatizada no verificador
do broker, com o publicador e o avaliador reais. Isso supre essa verificação
de integração sem modificar os arquivos sob responsabilidade de Clara.

## Fechamento conjunto

1. Publicar e integrar os commits desta etapa.
2. Clara repetir a avaliação contra a revisão integrada, usando a consulta
   de estado para observar também as pendências persistidas.
3. Mariana incorporar os gráficos e a avaliação final ao relatório.
4. Conferir os comandos em ambiente limpo e ensaiar a apresentação e a defesa
   individual. Cada integrante deve explicar suas decisões e as limitações.
5. Identificar a versão final do código e preparar a entrega no formato pedido
   pelo professor.

O relatório atual é a base técnica consolidada. A entrega acadêmica completa
ainda depende dos gráficos, da análise final e da revisão das duas integrantes.
