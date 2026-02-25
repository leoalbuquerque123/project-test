"""
Factory para SparkSession com Iceberg + GCS.

Este modulo encapsula toda a complexidade de criar uma SparkSession
configurada para usar Apache Iceberg com Google Cloud Storage.

=== PROBLEMA QUE ESTE MODULO RESOLVE ===

Existem DOIS cenarios de execucao que precisam funcionar:

  1. Via spark-submit (producao/Docker):
     $ spark-submit --conf spark.sql.catalog.local.warehouse=gs://bucket/warehouse ...
     → O Spark ja recebe TODAS as configs via linha de comando.
     → Se tentarmos sobrescrever com builder.config(), o Iceberg ignora
       porque o catalog ja foi inicializado com as configs do CLI.
     → Solucao: detectar que ja tem config e RETORNAR a sessao existente.

  2. Via pytest / pyspark shell / execucao direta:
     $ pytest tests/
     $ python src/jobs/bronze_transform.py
     → NAO tem configs pre-setadas. Precisamos configurar tudo no builder.
     → Solucao: criar sessao nova com configs programaticas.

=== FLUXO DE DECISAO ===

  get_spark_session()
       │
       ▼
  getOrCreate() — pega sessao existente (se houver)
       │
       ▼
  spark.conf.get("spark.sql.catalog.local.warehouse")
       │
       ├── TEM valor? → spark-submit ja configurou → RETORNA sessao
       │
       └── NAO tem? → precisa configurar manualmente
             │
             ▼
           spark.stop() → cria nova sessao com builder.config(...)

=== SERVICE ACCOUNT: sa-processing ===

Este modulo usa a SA de PROCESSAMENTO (sa-processing), que tem:
  - objectViewer no bucket de LANDING (so leitura dos raw files)
  - objectAdmin no bucket de WAREHOUSE (le e escreve Iceberg)

A SA de ingestao (sa-ingestion) NAO e usada aqui — ela e exclusiva
do download.py. Separar as SAs garante menor privilegio.

=== DEPENDENCIA: gcs-connector-shaded.jar ===

Para o Spark ler/escrever gs://, ele precisa do GCS connector JAR.
Esse JAR e baixado pelo docker-compose.yml na inicializacao:
  curl -sL -o /opt/spark/jars/gcs-connector-shaded.jar ...

NAO usamos iceberg-gcp-bundle porque ele NAO inclui o GCS Hadoop connector.
O gcs-connector-hadoop3-2.2.22-shaded.jar e a versao correta para Spark 3.5
(a versao "shaded" evita conflitos de protobuf com o Spark).
"""
import os
from typing import Optional

from pyspark.sql import SparkSession


def get_spark_session(
    app_name: str = "nyc-taxi-pipeline",
    warehouse_path: Optional[str] = None,
) -> SparkSession:
    """
    Retorna SparkSession com Iceberg + GCS configurado.

    Detecta automaticamente se esta rodando via spark-submit (configs ja setadas)
    ou direto (pytest, pyspark shell) e configura adequadamente.

    Args:
        app_name: Nome da aplicacao Spark (aparece no Spark UI).
        warehouse_path: Path do warehouse Iceberg. Se None, usa a variavel
                        de ambiente WAREHOUSE_PATH ou fallback para data/warehouse
                        (util para testes locais sem GCS).

    Returns:
        SparkSession configurada com Iceberg catalog e GCS (se warehouse gs://).

    Nota sobre Optional[str] vs str | None:
        Usamos Optional[str] (do typing) porque o container Docker roda
        Python 3.8, que nao suporta a sintaxe "str | None" (Python 3.10+).
    """
    # -------------------------------------------------------------------------
    # PASSO 1: Tentar pegar sessao existente
    # -------------------------------------------------------------------------
    # getOrCreate() retorna a sessao ativa se existir. Quando executado via
    # spark-submit, o Spark ja criou uma sessao com as configs --conf do CLI.
    spark = SparkSession.builder.appName(app_name).getOrCreate()

    # -------------------------------------------------------------------------
    # PASSO 2: Verificar se o catalog Iceberg ja esta configurado
    # -------------------------------------------------------------------------
    # Se spark-submit setou --conf spark.sql.catalog.local.warehouse=gs://...,
    # entao o catalog ja foi inicializado com esse path. Nao devemos sobrescrever.
    #
    # Por que nao podemos simplesmente chamar builder.config(...).getOrCreate()?
    # Porque o Iceberg inicializa o catalog na PRIMEIRA vez que a config e lida.
    # Mesmo que o builder.config() setasse um novo warehouse path, o catalog
    # ja estaria apontando para o path original do spark-submit.
    # Resultado: Iceberg escreve no lugar errado (local em vez de GCS).
    try:
        existing_warehouse = spark.conf.get("spark.sql.catalog.local.warehouse")
        if existing_warehouse:
            # spark-submit ja configurou tudo — usar a sessao como esta
            return spark
    except Exception:
        # Config nao existe — sessao foi criada sem configs Iceberg
        pass

    # -------------------------------------------------------------------------
    # PASSO 3: Configurar manualmente (pytest, pyspark shell, execucao direta)
    # -------------------------------------------------------------------------
    # Se chegou aqui, nao tem configs do spark-submit.
    # Precisamos parar a sessao "vazia" e criar uma nova com todas as configs.
    spark.stop()

    # Warehouse path: usa parametro > variavel de ambiente > fallback local
    # O fallback "data/warehouse" permite rodar testes locais sem GCS.
    if warehouse_path is None:
        warehouse_path = os.environ.get("WAREHOUSE_PATH", "data/warehouse")

    # Chave da SA de processamento (sa-processing)
    # Dentro do Docker: /opt/spark/work-dir/credentials/sa-processing-key.json
    # Fora do Docker: credentials/sa-processing-key.json
    gcs_key_path = os.environ.get(
        "GOOGLE_APPLICATION_CREDENTIALS",
        "credentials/sa-processing-key.json",
    )

    # -------------------------------------------------------------------------
    # PASSO 4: Builder com configs Iceberg
    # -------------------------------------------------------------------------
    builder = (
        SparkSession.builder
        .appName(app_name)
        # --- Iceberg Runtime JAR ---
        # Este pacote Maven contem o runtime do Iceberg para Spark 3.5.
        # O "_2.12" refere-se a versao do Scala (Spark 3.5 usa Scala 2.12).
        # O Spark baixa esse JAR automaticamente do Maven Central na primeira execucao.
        .config(
            "spark.jars.packages",
            "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.5.0",
        )
        # --- Iceberg Catalog "local" ---
        # SparkCatalog e a implementacao do Iceberg que se integra com o Spark SQL.
        # Tipo "hadoop" = usa o filesystem direto (GCS/HDFS/local) sem metastore.
        # Alternativa seria "hive" (precisa de Hive Metastore rodando — mais complexo).
        .config("spark.sql.catalog.local", "org.apache.iceberg.spark.SparkCatalog")
        .config("spark.sql.catalog.local.type", "hadoop")
        .config("spark.sql.catalog.local.warehouse", warehouse_path)
        # --- Iceberg SQL Extensions ---
        # Habilita comandos SQL especificos do Iceberg:
        #   - CALL local.system.rewrite_data_files(...)  — compactacao
        #   - ALTER TABLE ... SET TBLPROPERTIES(...)      — mudar configs
        #   - SELECT * FROM table.snapshots               — time travel metadata
        .config(
            "spark.sql.extensions",
            "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
        )
        # --- Default Catalog ---
        # Sem isso, toda query precisaria do prefixo "local.":
        #   spark.sql("SELECT * FROM local.bronze.trips")
        # Com defaultCatalog=local, podemos omitir (mas mantemos por clareza).
        .config("spark.sql.defaultCatalog", "local")
    )

    # -------------------------------------------------------------------------
    # PASSO 5: Configs GCS (somente se warehouse e gs://)
    # -------------------------------------------------------------------------
    # Essas configs dizem ao Hadoop COMO ler/escrever URIs gs://.
    # Sem elas, o Spark nao sabe o que fazer com "gs://bucket/path".
    #
    # O gcs-connector-shaded.jar (baixado no docker-compose.yml) contem
    # as classes GoogleHadoopFileSystem e GoogleHadoopFS.
    #
    # Se o warehouse e local (data/warehouse), nao precisa de nada disso.
    if warehouse_path.startswith("gs://"):
        builder = (
            builder
            # fs.gs.impl: implementacao do FileSystem para o scheme "gs://"
            # Sem isso: "No FileSystem for scheme: gs"
            .config(
                "spark.hadoop.fs.gs.impl",
                "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem",
            )
            # AbstractFileSystem.gs.impl: implementacao abstrata (usado internamente)
            .config(
                "spark.hadoop.fs.AbstractFileSystem.gs.impl",
                "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS",
            )
            # Autenticacao via Service Account (sa-processing)
            # Usa a chave JSON da SA montada no container via Docker volume
            .config(
                "spark.hadoop.google.cloud.auth.service.account.enable", "true",
            )
            .config(
                "spark.hadoop.google.cloud.auth.service.account.json.keyfile",
                gcs_key_path,
            )
        )

    return builder.getOrCreate()