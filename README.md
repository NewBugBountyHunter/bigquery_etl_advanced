# Student Data Pipeline (PySpark) - Guia Completo

Projeto didático para processar dados de estudantes em `data/raw/bi.csv` usando **Python + PySpark**.
Focado em aprendizagem de pipelines de dados, SQL avançado (CTEs, Window Functions, arrays/JSON), BigQuery, dbt e práticas de engenharia de dados.

---

## 1️⃣ Estrutura do Projeto

```text
data/raw/bi.csv          # CSV de entrada
notebooks/               # Notebooks de exploração e análise
analysis.ipynb
src/                     # Código fonte
  etl/
    __init__.py
    jobs.py              # Limpeza e transformação dos dados
    schema.py            # Schema explícito do CSV
  utils/
    io.py                # Funções de leitura/escrita
    qc.py                # Checks de qualidade
  main.py                # Orquestração do pipeline
sql/
  example_ctes.sql        # Exemplo de queries SQL com CTEs
tests/
  test_etl.py             # Testes unitários com PySpark
README.md
requirements.txt          # Dependências Python
```

---

## 2️⃣ Requisitos

* Python 3.8+
* Java 11 (recomendado para PySpark)
* pip (gerenciador de pacotes Python)

---

## 3️⃣ Instalação do Ambiente Virtual

### Linux / Mac

```bash
# Cria um ambiente virtual chamado "venv"
python -m venv venv

# Ativa o ambiente virtual
source venv/bin/activate

# Instala dependências
pip install -r requirements.txt
```

### Windows / PowerShell

```powershell
# Cria o ambiente virtual
python -m venv venv

# Ativa o ambiente virtual
venv\Scripts\Activate.ps1

# Caso receba erro de execução temporariamente permita scripts
Set-ExecutionPolicy -Scope Process -ExecutionPolicy Bypass

# Instala dependências
pip install -r requirements.txt
```

**Explicações detalhadas:**

* `python -m venv venv` → cria um ambiente isolado onde pacotes instalados não interferem no Python do sistema.
* `source venv/bin/activate` / `venv\Scripts\Activate.ps1` → ativa o ambiente virtual, direcionando todos os comandos Python/pip para ele.
* `pip install -r requirements.txt` → instala todas as bibliotecas necessárias (PySpark, pytest, pyarrow).
* `Set-ExecutionPolicy ...` → apenas no Windows para permitir rodar scripts do virtualenv sem alterar configurações globais.

**Desativar ambiente virtual:**

```bash
deactivate   # Linux/Mac ou Windows
```

---

## 4️⃣ Execução do Pipeline

Rodando o pipeline principal:

```bash
python src/main.py --input data/raw/bi.csv --output data/output --use_schema
```

**Parâmetros:**

* `--input` → caminho do arquivo CSV de entrada
* `--output` → pasta onde os dados transformados serão salvos
* `--use_schema` → usa schema explícito do CSV (opcional, mas recomendado para segurança de tipos)

---

## 5️⃣ Explicação do Código Principal (`main.py`)

### 5.1 SparkSession

```python
spark = SparkSession.builder \
    .appName("student-etl") \
    .config("spark.sql.shuffle.partitions", "8") \
    .getOrCreate()
```

**Explicação:**

* `SparkSession.builder` → cria uma sessão Spark, ponto de entrada para todos os jobs PySpark.
* `.appName("student-etl")` → nome da aplicação, usado em logs e interface do Spark.
* `.config("spark.sql.shuffle.partitions", "8")` → define quantas partições usar durante operações de shuffle (join, agregações).
* `.getOrCreate()` → cria a sessão ou retorna a sessão existente.

---

### 5.2 Leitura do CSV

```python
df = spark.read.option("header","true") \
               .option("inferSchema","true") \
               .option("encoding","ISO-8859-1") \
               .option("nullValue","null") \
               .csv(path)
```

* `header="true"` → primeira linha é tratada como nomes de colunas.
* `inferSchema="true"` → Spark tenta detectar tipo de cada coluna automaticamente.
* `encoding="ISO-8859-1"` → codificação usada pelo CSV (acentos corretos).
* `nullValue="null"` → trata a palavra `null` como valor nulo.

**Com schema explícito:**

```python
df = spark.read.option("header","true") \
               .option("encoding","ISO-8859-1") \
               .schema(student_schema) \
               .csv(path)
```

* `.schema(student_schema)` → força o tipo de cada coluna, evita erros de inferência.

---

### 5.3 Limpeza de Dados (`clean`)

```python
df2 = df.withColumn("fNAME", trim(col("fNAME"))) \
        .withColumn("lNAME", trim(col("lNAME"))) \
        .withColumn("gender",
                    when(lower(trim(col("gender"))).isin("m","male"), lit("Male"))
                    .when(lower(trim(col("gender"))).isin("f","female"), lit("Female"))
                    .otherwise(lit("Unknown"))) \
        .withColumn("Python", col("Python").cast(DoubleType())) \
        .withColumn("DB", col("DB").cast(IntegerType())) \
        .withColumn("studyHOURS", when(col("studyHOURS") < 0, lit(0)).otherwise(col("studyHOURS"))) \
        .na.fill({"Python": -1.0})
```

* `withColumn("fNAME", trim(col("fNAME")))` → remove espaços extras do nome.
* `lower(trim(col("gender")))` → transforma para minúsculas e remove espaços.
* `when(...).otherwise(...)` → condicional para normalizar gênero.
* `cast(DoubleType())` / `cast(IntegerType())` → força tipos corretos para cálculos.
* `na.fill({"Python": -1.0})` → preenche valores nulos com -1.0, usado como marca de missing.

---

### 5.4 Transformação Avançada (`transform`)

```python
df = df.withColumn("avg_score", (col("Python")+col("DB"))/2.0)
```

* Cria coluna `avg_score` como média de Python e DB.

```python
subjects_arr = array(
    struct(lit("Python").alias("subject"), col("Python").alias("score")),
    struct(lit("DB").alias("subject"), col("DB").alias("score"))
)
df = df.withColumn("subjects", subjects_arr)
```

* `array(...)` → cria **array** de structs (estrutura com `subject` e `score`).
* `struct(...)` → cria registro composto (`subject`, `score`).
* `lit("Python")` → literal, valor fixo.
* `alias("subject")` → renomeia coluna.

```python
df_exploded = df.select(..., explode(col("subjects")).alias("sub"))
```

* `explode()` → transforma cada elemento do array em **uma linha separada** (formato long), útil para ranking e análise por disciplina.

---

### 5.5 Window Functions

```python
w = Window.partitionBy("country","subject").orderBy(desc("score"))
df_ranked = df_exploded.withColumn("rank_in_country", row_number().over(w))
```

* `Window.partitionBy(...)` → cria janela de cálculo por país e disciplina.
* `.orderBy(desc("score"))` → ordena por score decrescente dentro da janela.
* `row_number().over(w)` → atribui número de ranking para cada estudante na janela.

```python
w2 = Window.partitionBy("subject")
df_ranked = df_ranked.withColumn("mean_score", avg("score").over(w2)) \
                     .withColumn("stddev_score", stddev("score").over(w2)) \
                     .withColumn("zscore", (col("score")-col("mean_score"))/col("stddev_score"))
```

* Calcula média, desvio padrão e **z-score** por disciplina, usando **funções de janela**.

---

### 5.6 Escrita em Parquet

```python
df.write.mode("overwrite").partitionBy("country").parquet(out_path)
```

* `mode("overwrite")` → sobrescreve arquivos existentes.
* `partitionBy("country")` → particiona dados por país, melhora performance.
* `.parquet(out_path)` → escreve em **formato columnar**, eficiente para grandes datasets.

---

## 6️⃣ Testes Unitários (`tests/test_etl.py`)

* Usa `pytest` para validar funções `clean()` e `transform()`.
* Cria pequenos DataFrames de exemplo e verifica:

  * Preenchimento de valores nulos
  * Colunas geradas após `explode` e ranking
  * Integridade de colunas e tipos

```python
assert "subject" in cols and "score" in cols
assert "rank_in_country" in cols
```

---

## 7️⃣ Qualidade de Dados (`utils/qc.py`)

* `check_not_empty(df)` → gera erro se DataFrame estiver vazio.
* `count_nulls(df)` → retorna dicionário coluna → número de nulos.
* `has_columns(df, required_cols)` → valida se todas as colunas necessárias existem.

---

## 8️⃣ Dicas para Aprendizado

* **SQL avançado:** pratique CTEs, Window Functions, arrays/JSON.
* **BigQuery + GCP:** use datasets públicos, teste pipelines batch/streaming.
* **dbt:** transforme dados, crie testes e documente modelos.
* **PySpark:** pratique local e depois em Dataproc ou Dataflow.
* **Git/GitHub:** versionamento, PRs e code reviews.
* **Snowflake/Iceberg:** ent
