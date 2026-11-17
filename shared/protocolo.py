"""Codificação e decodificação de quadros do protocolo Publish/Subscribe."""

import math
import struct
from dataclasses import dataclass
from enum import IntEnum
from uuid import UUID


class Operacao(IntEnum):
    IDENTIFICAR = 1
    CRIAR_TOPICO = 2
    EXCLUIR_TOPICO = 3
    INSCREVER = 4
    CANCELAR_INSCRICAO = 5
    PUBLICAR = 6
    ENTREGAR = 7
    ACK_PUBLICACAO = 8
    ACK_ENTREGA = 9
    RESPOSTA = 10


class Resultado(IntEnum):
    OK = 0
    DESCARTADA_SEM_INSCRITOS = 1
    TOPICO_COM_PENDENCIAS = 2
    INSCRICAO_COM_PENDENCIAS = 3
    TOPICO_INEXISTENTE = 4
    TOPICO_JA_EXISTENTE = 5
    CLIENTE_ID_EM_USO = 6
    REQUISICAO_INVALIDA = 7


class ErroProtocolo(ValueError):
    """Campo inválido ou quadro malformado."""


@dataclass(frozen=True)
class Mensagem:
    operacao: Operacao
    id_solicitacao: int
    campos: dict


TAMANHO_CONTEUDO = struct.Struct(">I")
CABECALHO = struct.Struct(">BI")
TAMANHO_TEXTO = struct.Struct(">H")
TEMPERATURA = struct.Struct(">f")
CODIGO_RESULTADO = struct.Struct(">B")
TAMANHO_UUID = 16
MAX_TEXTO = 0xFFFF
MAX_ID_SOLICITACAO = 0xFFFFFFFF
MAX_CONTEUDO = (
    CABECALHO.size + TAMANHO_UUID + TAMANHO_TEXTO.size + MAX_TEXTO + TEMPERATURA.size
)

_FORMATO_PUBLICACAO = ("uuid", "topico", "temperatura")
_CAMPOS = {
    Operacao.IDENTIFICAR: ("cliente_id",),
    Operacao.CRIAR_TOPICO: ("topico",),
    Operacao.EXCLUIR_TOPICO: ("topico",),
    Operacao.INSCREVER: ("topico",),
    Operacao.CANCELAR_INSCRICAO: ("topico",),
    Operacao.PUBLICAR: _FORMATO_PUBLICACAO,
    Operacao.ENTREGAR: _FORMATO_PUBLICACAO,
    Operacao.ACK_PUBLICACAO: ("uuid", "resultado"),
    Operacao.ACK_ENTREGA: ("uuid", "cliente_id"),
    Operacao.RESPOSTA: ("resultado", "mensagem"),
}


def _validar_codigo(tipo, valor):
    if isinstance(valor, int) and not isinstance(valor, bool):
        try:
            return tipo(valor)
        except ValueError:
            pass
    raise ErroProtocolo(f"Código de {tipo.__name__.lower()} inválido: {valor!r}")


def _validar_id(operacao, id_solicitacao):
    if (
        not isinstance(id_solicitacao, int)
        or isinstance(id_solicitacao, bool)
        or not 0 <= id_solicitacao <= MAX_ID_SOLICITACAO
    ):
        raise ErroProtocolo("ID de solicitação fora do intervalo de uint32")
    exige_zero = operacao in (Operacao.ENTREGAR, Operacao.ACK_ENTREGA)
    if (id_solicitacao == 0) != exige_zero:
        raise ErroProtocolo(f"ID de solicitação inválido para {operacao.name}")


def _codificar_campo(nome, valor):
    if nome == "uuid":
        if not isinstance(valor, UUID):
            raise ErroProtocolo("O campo uuid deve ser um uuid.UUID")
        return valor.bytes
    if nome == "resultado":
        return CODIGO_RESULTADO.pack(_validar_codigo(Resultado, valor))
    if nome == "temperatura":
        if not isinstance(valor, (int, float)) or isinstance(valor, bool):
            raise ErroProtocolo("A temperatura deve ser numérica")
        try:
            if math.isfinite(valor):
                return TEMPERATURA.pack(valor)
        except (OverflowError, struct.error):
            pass
        raise ErroProtocolo("A temperatura deve ser finita e caber em float32")
    if not isinstance(valor, str):
        raise ErroProtocolo(f"O campo {nome} deve ser texto")
    try:
        dados = valor.encode("utf-8")
    except UnicodeEncodeError as erro:
        raise ErroProtocolo(f"Texto inválido em {nome}") from erro
    if len(dados) > MAX_TEXTO or (not dados and nome != "mensagem"):
        raise ErroProtocolo(f"Tamanho de texto inválido em {nome}")
    return TAMANHO_TEXTO.pack(len(dados)) + dados


def codificar(operacao, id_solicitacao, **campos):
    """Retorna um quadro completo, incluindo o prefixo de tamanho."""
    operacao = _validar_codigo(Operacao, operacao)
    _validar_id(operacao, id_solicitacao)
    nomes = _CAMPOS[operacao]
    if operacao == Operacao.RESPOSTA:
        campos.setdefault("mensagem", "")
    if campos.keys() != set(nomes):
        raise ErroProtocolo(f"Campos esperados para {operacao.name}: {', '.join(nomes)}")
    corpo = CABECALHO.pack(operacao, id_solicitacao)
    corpo += b"".join(_codificar_campo(nome, campos[nome]) for nome in nomes)
    return TAMANHO_CONTEUDO.pack(len(corpo)) + corpo


def decodificar(quadro):
    """Decodifica exatamente um quadro completo do tipo bytes."""
    if not isinstance(quadro, bytes):
        raise ErroProtocolo("O quadro deve ser do tipo bytes")
    if len(quadro) < TAMANHO_CONTEUDO.size:
        raise ErroProtocolo("Prefixo de tamanho incompleto")
    (tamanho,) = TAMANHO_CONTEUDO.unpack_from(quadro)
    if not CABECALHO.size <= tamanho <= MAX_CONTEUDO:
        raise ErroProtocolo("Tamanho de conteúdo fora dos limites")
    if len(quadro) != TAMANHO_CONTEUDO.size + tamanho:
        raise ErroProtocolo("Tamanho do quadro difere do prefixo")

    codigo, id_solicitacao = CABECALHO.unpack_from(quadro, TAMANHO_CONTEUDO.size)
    operacao = _validar_codigo(Operacao, codigo)
    _validar_id(operacao, id_solicitacao)
    posicao = TAMANHO_CONTEUDO.size + CABECALHO.size

    def ler(quantidade):
        nonlocal posicao
        fim = posicao + quantidade
        if fim > len(quadro):
            raise ErroProtocolo("Campo incompleto")
        dados = quadro[posicao:fim]
        posicao = fim
        return dados

    campos = {}
    for nome in _CAMPOS[operacao]:
        if nome == "uuid":
            valor = UUID(bytes=ler(TAMANHO_UUID))
        elif nome == "resultado":
            (codigo,) = CODIGO_RESULTADO.unpack(ler(CODIGO_RESULTADO.size))
            valor = _validar_codigo(Resultado, codigo)
        elif nome == "temperatura":
            (valor,) = TEMPERATURA.unpack(ler(TEMPERATURA.size))
            if not math.isfinite(valor):
                raise ErroProtocolo("Temperatura não finita")
        else:
            (tamanho,) = TAMANHO_TEXTO.unpack(ler(TAMANHO_TEXTO.size))
            if tamanho == 0 and nome != "mensagem":
                raise ErroProtocolo(f"O campo {nome} não pode ser vazio")
            try:
                valor = ler(tamanho).decode("utf-8")
            except UnicodeDecodeError as erro:
                raise ErroProtocolo(f"UTF-8 inválido em {nome}") from erro
        campos[nome] = valor
    if posicao != len(quadro):
        raise ErroProtocolo("Bytes excedentes após os campos da mensagem")
    return Mensagem(operacao, id_solicitacao, campos)
