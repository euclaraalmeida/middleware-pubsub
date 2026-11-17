import unittest
from uuid import UUID

from shared.protocolo import (
    MAX_CONTEUDO,
    MAX_ID_SOLICITACAO,
    MAX_TEXTO,
    ErroProtocolo,
    Mensagem,
    Operacao,
    Resultado,
    codificar,
    decodificar,
)


UUID_PUBLICACAO = UUID("00112233-4455-6677-8899-aabbccddeeff")


def quadro_de_teste(conteudo):
    return len(conteudo).to_bytes(4, "big") + conteudo


class TestProtocolo(unittest.TestCase):
    def test_quadros_das_dez_operacoes(self):
        uuid_hex = "00112233445566778899aabbccddeeff"
        topico = {"topico": "é"}
        publicacao = {"uuid": UUID_PUBLICACAO, "topico": "é", "temperatura": 25.5}
        casos = [
            (Operacao.IDENTIFICAR, 1, {"cliente_id": "s1"}, "01 00000001 0002 7331"),
            (Operacao.CRIAR_TOPICO, 1, topico, "02 00000001 0002 c3a9"),
            (Operacao.EXCLUIR_TOPICO, 1, topico, "03 00000001 0002 c3a9"),
            (Operacao.INSCREVER, 1, topico, "04 00000001 0002 c3a9"),
            (Operacao.CANCELAR_INSCRICAO, 1, topico, "05 00000001 0002 c3a9"),
            (Operacao.PUBLICAR, 2, publicacao, f"06 00000002 {uuid_hex} 0002 c3a9 41cc0000"),
            (Operacao.ENTREGAR, 0, publicacao, f"07 00000000 {uuid_hex} 0002 c3a9 41cc0000"),
            (
                Operacao.ACK_PUBLICACAO, 2,
                {"uuid": UUID_PUBLICACAO, "resultado": Resultado.OK},
                f"08 00000002 {uuid_hex} 00",
            ),
            (
                Operacao.ACK_ENTREGA, 0,
                {"uuid": UUID_PUBLICACAO, "cliente_id": "s1"},
                f"09 00000000 {uuid_hex} 0002 7331",
            ),
            (
                Operacao.RESPOSTA, 1,
                {"resultado": Resultado.TOPICO_INEXISTENTE, "mensagem": "não"},
                "0a 00000001 04 0004 6ec3a36f",
            ),
        ]
        for operacao, id_solicitacao, campos, conteudo in casos:
            with self.subTest(operacao=operacao):
                esperado = quadro_de_teste(bytes.fromhex(conteudo))
                self.assertEqual(codificar(operacao, id_solicitacao, **campos), esperado)
                self.assertEqual(decodificar(esperado), Mensagem(operacao, id_solicitacao, campos))

    def test_resultados_sem_mensagem(self):
        for codigo, resultado in enumerate(Resultado):
            with self.subTest(resultado=resultado):
                quadro = codificar(Operacao.RESPOSTA, 1, resultado=resultado)
                self.assertEqual(quadro, bytes.fromhex("00000008 0a 00000001") + bytes([codigo, 0, 0]))
                self.assertIs(decodificar(quadro).campos["resultado"], resultado)
                self.assertEqual(decodificar(quadro).campos["mensagem"], "")

    def test_uuid_preservado_ao_retransmitir(self):
        quadro = codificar(
            Operacao.PUBLICAR, 1, uuid=UUID_PUBLICACAO, topico="sensor", temperatura=-12.5
        )
        retransmissao = codificar(Operacao.PUBLICAR, 2, **decodificar(quadro).campos)
        self.assertEqual(decodificar(retransmissao).campos["uuid"], UUID_PUBLICACAO)
        self.assertEqual(quadro[9:], retransmissao[9:])

    def test_limites_de_texto_e_id(self):
        topico = "é" * (MAX_TEXTO // 2) + "x"
        quadro = codificar(
            Operacao.PUBLICAR, MAX_ID_SOLICITACAO,
            uuid=UUID_PUBLICACAO, topico=topico, temperatura=0,
        )
        self.assertEqual(len(quadro) - 4, MAX_CONTEUDO)
        mensagem = decodificar(quadro)
        self.assertEqual(mensagem.id_solicitacao, MAX_ID_SOLICITACAO)
        self.assertEqual(mensagem.campos["topico"], topico)
        with self.assertRaises(ErroProtocolo):
            codificar(Operacao.CRIAR_TOPICO, 1, topico=topico + "x")

    def test_rejeita_campos_invalidos_na_codificacao(self):
        casos = [
            (255, 1, {}),
            (Operacao.IDENTIFICAR, 0, {"cliente_id": "s1"}),
            (Operacao.IDENTIFICAR, -1, {"cliente_id": "s1"}),
            (Operacao.IDENTIFICAR, MAX_ID_SOLICITACAO + 1, {"cliente_id": "s1"}),
            (Operacao.IDENTIFICAR, True, {"cliente_id": "s1"}),
            (Operacao.IDENTIFICAR, 1, {"cliente_id": ""}),
            (Operacao.IDENTIFICAR, 1, {"cliente_id": "\ud800"}),
            (Operacao.IDENTIFICAR, 1, {}),
            (Operacao.IDENTIFICAR, 1, {"cliente_id": "s1", "extra": "s2"}),
            (Operacao.ACK_ENTREGA, 1, {"uuid": UUID_PUBLICACAO, "cliente_id": "s1"}),
            (Operacao.ACK_PUBLICACAO, 1, {"uuid": b"123", "resultado": Resultado.OK}),
            (Operacao.RESPOSTA, 1, {"resultado": 255}),
        ]
        for operacao, identificador, campos in casos:
            with self.subTest(operacao=operacao, identificador=identificador, campos=campos):
                with self.assertRaises(ErroProtocolo):
                    codificar(operacao, identificador, **campos)

    def test_rejeita_temperaturas_invalidas(self):
        for temperatura in (float("nan"), float("inf"), -float("inf"), 1e40, "25", True):
            with self.subTest(temperatura=temperatura):
                with self.assertRaises(ErroProtocolo):
                    codificar(
                        Operacao.PUBLICAR, 1,
                        uuid=UUID_PUBLICACAO, topico="sensor", temperatura=temperatura,
                    )

    def test_rejeita_quadros_invalidos(self):
        valido = codificar(Operacao.IDENTIFICAR, 1, cliente_id="s1")
        conteudos_invalidos = [
            "01",
            "ff 00000001",
            "01 00000000 0001 61",
            "09 00000001",
            "01 00000001 0002 61",
            "01 00000001 0001 ff",
            "01 00000001 0000",
            "01 00000001 0001 61 ff",
            "06 00000001 001122",
            "06 00000001 00112233445566778899aabbccddeeff 0001 61 7f800000",
            "0a 00000001 ff 0000",
            "0a 00000001 00",
        ]
        quadros = [valido[:i] for i in range(len(valido))]
        quadros.extend([valido + valido, (MAX_CONTEUDO + 1).to_bytes(4, "big")])
        quadros.extend(quadro_de_teste(bytes.fromhex(hexadecimal)) for hexadecimal in conteudos_invalidos)
        for quadro in quadros:
            with self.subTest(quadro=quadro.hex()):
                with self.assertRaises(ErroProtocolo):
                    decodificar(quadro)


if __name__ == "__main__":
    unittest.main()
