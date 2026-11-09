import hashlib
import json
import logging
from datetime import timedelta
from pathlib import Path

import pendulum
import requests
from airflow.sdk import dag, task, get_current_context


PASTA_CHEGADA = Path("/root/reencontra/chegada")


def salvar_json(fonte, registros):
    """Salva um arquivo por fonte e execucao, com data no nome."""
    contexto = get_current_context()
    execucao = contexto["dag_run"]

    # O identificador permanece igual quando uma tarefa e repetida.
    lote_id = hashlib.sha256(
        execucao.run_id.encode("utf-8")
    ).hexdigest()[:16]

    data = execucao.start_date.strftime("%Y-%m-%d")

    pasta = PASTA_CHEGADA / fonte
    pasta.mkdir(parents=True, exist_ok=True)

    destino = pasta / f"{fonte}_{data}_{lote_id}.json"
    temporario = destino.with_suffix(".tmp")

    temporario.write_text(
        json.dumps(registros, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    temporario.replace(destino)

    logging.info(
        "Fonte=%s | registros=%s | arquivo=%s",
        fonte,
        len(registros),
        destino.name,
    )

    return str(destino)


@dag(
    dag_id="reencontra_coleta",
    description="Coleta IBGE e estatisticas reais de desaparecimentos de SP",
    start_date=pendulum.datetime(
        2026, 11, 7, tz="America/Sao_Paulo"
    ),
    schedule="0 8 * * *",
    catchup=False,
    max_active_runs=1,
    default_args={
        "retries": 2,
        "retry_delay": timedelta(seconds=30),
    },
    tags=["reencontra", "big-data"],
)
def reencontra_coleta():

    @task
    def coletar_municipios():
        url = (
            "https://servicodados.ibge.gov.br"
            "/api/v1/localidades/estados/35/municipios"
        )

        resposta = requests.get(url, timeout=60)
        resposta.raise_for_status()
        municipios = resposta.json()

        if not isinstance(municipios, list) or not municipios:
            raise ValueError("IBGE retornou uma resposta vazia ou invalida.")

        registros = [
            {
                "municipio_id": municipio["id"],
                "municipio_nome": municipio["nome"],
                "uf": "SP",
                "fonte": "IBGE",
                "url_fonte": url,
            }
            for municipio in municipios
        ]

        return salvar_json("municipios_ibge", registros)


    @task
    def coletar_desaparecimentos():
        import io
        import re
        import openpyxl

        url = (
            "https://www.gov.br/mj/pt-br/assuntos/sua-seguranca/"
            "seguranca-publica/estatistica/download/desaparecidos/"
            "base-de-dados-desaparecidos-2023_2024_2025.xlsx"
        )

        resposta = requests.get(url, timeout=90)
        resposta.raise_for_status()

        livro = openpyxl.load_workbook(
            io.BytesIO(resposta.content),
            read_only=True,
            data_only=True,
        )

        linhas = livro.active.iter_rows(values_only=True)
        cabecalhos = next(linhas)

        if tuple(cabecalhos[:2]) != ("Ano", "UF"):
            raise ValueError("A estrutura da planilha mudou.")

        registros = []
        anos = set()

        try:
            for linha in linhas:
                if str(linha[1]).strip().upper() != "SP":
                    continue

                ano = int(linha[0])
                anos.add(ano)

                for indice, cabecalho in enumerate(cabecalhos):
                    correspondencia = re.match(
                        r"^t([13])\.(\d+)\.(\d+)\s*-",
                        str(cabecalho),
                    )

                    if not correspondencia:
                        continue

                    tabela, grupo, mes = map(
                        int, correspondencia.groups()
                    )

                    if not (1 <= grupo <= 15 and 1 <= mes <= 12):
                        raise ValueError("Codigo de indicador inesperado.")

                    faixas = [
                        "crianca", "adolescente", "adulto",
                        "idoso", "nao_informada",
                    ]
                    sexos = [
                        "masculino", "feminino", "nao_informado",
                    ]

                    valor = linha[indice]
                    registros.append({
                        "ano": ano,
                        "uf": "SP",
                        "mes": mes,
                        "indicador": (
                            "desaparecimento" if tabela == 1
                            else "localizacao"
                        ),
                        "faixa_etaria": faixas[(grupo - 1) // 3],
                        "sexo": sexos[(grupo - 1) % 3],
                        "quantidade_original": (
                            None if valor is None else str(valor)
                        ),
                        "campo_origem": str(cabecalho),
                        "fonte": "MJSP",
                        "url_fonte": url,
                        "dado_sintetico": False,
                    })
        finally:
            livro.close()

        if not {2023, 2024, 2025}.issubset(anos):
            raise ValueError("Anos esperados de SP nao encontrados.")

        if len(registros) != len(anos) * 360:
            raise ValueError("Quantidade inesperada de indicadores.")

        return salvar_json("desaparecimentos_sp", registros)

    @task
    def conferir_arquivos(arquivo_municipios, arquivo_atendimentos):
        for caminho in [arquivo_municipios, arquivo_atendimentos]:
            arquivo = Path(caminho)

            if not arquivo.is_file():
                raise FileNotFoundError(
                    f"Arquivo esperado nao encontrado: {arquivo.name}"
                )

            registros = json.loads(
                arquivo.read_text(encoding="utf-8")
            )

            if not isinstance(registros, list) or not registros:
                raise ValueError(
                    f"Arquivo vazio ou invalido: {arquivo.name}"
                )

            logging.info(
                "Conferencia OK | arquivo=%s | registros=%s",
                arquivo.name,
                len(registros),
            )

    municipios = coletar_municipios()
    atendimentos = coletar_desaparecimentos()

    conferir_arquivos(municipios, atendimentos)


reencontra_coleta()