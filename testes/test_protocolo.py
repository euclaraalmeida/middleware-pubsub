import unittest
from uuid import UUID

from shared.protocolo import (
    AcumuladorQuadros,
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


class TestAcumuladorQuadros(unittest.TestCase):
    def setUp(self):
        self.quadros = [
            codificar(Operacao.IDENTIFICAR, 1, cliente_id="s1"),
            codificar(Operacao.ENTREGAR, 0, uuid=UUID_PUBLICACAO, topico="névoa", temperatura=-12.5),
            codificar(Operacao.RESPOSTA, 2, resultado=Resultado.OK, mensagem="inscrição aceita"),
        ]
        self.mensagens = [decodificar(quadro) for quadro in self.quadros]
        self.acumulador = AcumuladorQuadros()

    def test_quadro_completo(self):
        self.assertEqual(list(self.acumulador.alimentar(self.quadros[0])), self.mensagens[:1])
        self.acumulador.finalizar()

    def test_quadro_em_dois_fragmentos(self):
        quadro = self.quadros[1]
        for corte in range(1, len(quadro)):
            with self.subTest(corte=corte):
                acumulador = AcumuladorQuadros()
                self.assertEqual(list(acumulador.alimentar(quadro[:corte])), [])
                self.assertEqual(list(acumulador.alimentar(quadro[corte:])), self.mensagens[1:2])
                acumulador.finalizar()

    def test_quadro_byte_a_byte(self):
        quadro = self.quadros[1]
        for byte in quadro[:-1]:
            self.assertEqual(list(self.acumulador.alimentar(bytes([byte]))), [])
        self.assertEqual(list(self.acumulador.alimentar(quadro[-1:])), self.mensagens[1:2])

    def test_quadros_concatenados_preservam_ordem_e_campos(self):
        self.assertEqual(
            list(self.acumulador.alimentar(b"".join(self.quadros))), self.mensagens
        )
        self.acumulador.finalizar()

    def test_quadro_completo_seguido_de_parcial(self):
        primeiro, segundo, _ = self.quadros
        self.assertEqual(
            list(self.acumulador.alimentar(primeiro + segundo[:2])), self.mensagens[:1]
        )
        self.assertEqual(list(self.acumulador.alimentar(b"")), [])
        self.assertEqual(
            list(self.acumulador.alimentar(segundo[2:])), self.mensagens[1:2]
        )

    def test_resto_de_quadro_seguido_de_novos_quadros(self):
        primeiro, segundo, terceiro = self.quadros
        self.assertEqual(list(self.acumulador.alimentar(primeiro[:-1])), [])
        self.assertEqual(
            list(self.acumulador.alimentar(primeiro[-1:] + segundo + terceiro)),
            self.mensagens,
        )

    def test_rejeita_prefixos_invalidos_assim_que_completos(self):
        for tamanho in (0, 4, MAX_CONTEUDO + 1, 0xFFFFFFFF):
            with self.subTest(tamanho=tamanho):
                acumulador = AcumuladorQuadros()
                prefixo = tamanho.to_bytes(4, "big")
                self.assertEqual(list(acumulador.alimentar(prefixo[:3])), [])
                with self.assertRaisesRegex(ErroProtocolo, "fora dos limites"):
                    list(acumulador.alimentar(prefixo[3:]))

    def test_quadro_malformado_interrompe_fluxo_apos_mensagens_validas(self):
        malformado = quadro_de_teste(bytes.fromhex("01 00000001 0001 ff"))
        mensagens = self.acumulador.alimentar(self.quadros[0] + malformado + self.quadros[1])
        self.assertEqual(next(mensagens), self.mensagens[0])
        with self.assertRaisesRegex(ErroProtocolo, "UTF-8 inválido"):
            next(mensagens)
        with self.assertRaises(ErroProtocolo):
            list(self.acumulador.alimentar(self.quadros[1]))

    def test_fim_do_fluxo_com_quadro_incompleto(self):
        for corte in (2, 4, 7):
            with self.subTest(corte=corte):
                acumulador = AcumuladorQuadros()
                self.assertEqual(list(acumulador.alimentar(self.quadros[0][:corte])), [])
                with self.assertRaisesRegex(ErroProtocolo, "quadro incompleto"):
                    acumulador.finalizar()


if __name__ == "__main__":
    unittest.main()
