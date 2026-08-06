"""Carrega o conjunto de referência de prazos e mede um extrator contra ele.

Separa três resultados que costumam ser confundidos num número só de
"acurácia", e que aqui têm consequências bem diferentes:

* **acerto** — devolveu a data anotada, ou absteve quando era para abster.
* **abstenção** — calou onde havia resposta. Custa cobertura, não custa
  confiança: a vaga aparece sem prazo, como aparecia antes de tudo isso.
* **erro** — devolveu data errada, ou devolveu data onde era para abster.
  Este é o único que não pode acontecer, porque é o que faz uma vaga aberta
  parecer encerrada.

Medir os três juntos permitiria trocar erro por cobertura sem ninguém
perceber — que é exatamente a troca que não queremos fazer.
"""
import gzip
import json
from dataclasses import dataclass, field
from pathlib import Path
from typing import Callable, List, Optional

FIXTURES = Path(__file__).parent / "fixtures"
PRAZOS_JSON = FIXTURES / "prazos.json"
EDITAIS_DIR = FIXTURES / "editais"


@dataclass(frozen=True)
class EditalAnotado:
    arquivo: str
    fonte: str
    link: str
    titulo: str
    #: None quer dizer "abster é a resposta certa" — ver o _leia_me do JSON.
    encerramento: Optional[str]
    nota: str
    #: Preenchido quando o edital oferecia mais de uma data e a anotação saiu
    #: de uma regra, não de concordância entre as partes do documento. Fica
    #: explícito para que a procedência da resposta seja auditável.
    resolvido_por: Optional[str] = None

    def texto(self) -> str:
        with gzip.open(EDITAIS_DIR / self.arquivo, "rt", encoding="utf-8") as f:
            return f.read()


@dataclass
class Avaliacao:
    acertos: int = 0
    abstencoes: int = 0
    erros: List[str] = field(default_factory=list)

    @property
    def total(self) -> int:
        return self.acertos + self.abstencoes + len(self.erros)

    def resumo(self) -> str:
        return (
            f"{self.acertos} acerto(s), {self.abstencoes} abstenção(ões), "
            f"{len(self.erros)} erro(s) em {self.total} editais"
        )


def carregar() -> List[EditalAnotado]:
    dados = json.loads(PRAZOS_JSON.read_text(encoding="utf-8"))
    return [EditalAnotado(**item) for item in dados["editais"]]


def avaliar(extrator: Callable[[str], Optional[str]]) -> Avaliacao:
    """Roda o extrator contra todo o conjunto e classifica cada resposta."""
    resultado = Avaliacao()

    for edital in carregar():
        obtido = extrator(edital.texto())
        esperado = edital.encerramento

        if obtido == esperado:
            resultado.acertos += 1
        elif obtido is None:
            resultado.abstencoes += 1
        elif esperado is None:
            resultado.erros.append(
                f"{edital.arquivo}: devolveu {obtido} onde era para abster ({edital.nota})"
            )
        else:
            resultado.erros.append(
                f"{edital.arquivo}: devolveu {obtido}, o edital diz {esperado}"
            )

    return resultado
