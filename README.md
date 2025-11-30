# 🏭 Go-Refinery

> Um pipeline de dados de alta performance, extensível e configurável escrito em Go.

O **Go-Refinery** é uma solução robusta para processamento de dados em tempo real. Ele foi projetado para ingerir dados de diversas fontes (como Kafka), processá-los através de uma cadeia de processadores configuráveis e entregá-los a destinos de armazenamento (como SQL Server) de forma eficiente e confiável.

---

## 🚀 Funcionalidades

- **Alta Performance**: Construído em Go, aproveitando goroutines para processamento concorrente massivo.
- **Arquitetura Modular**: Design baseado em componentes (Source, Processors, Sink) facilitando a extensão.
- **Configuração via YAML**: Defina todo o pipeline, desde a conexão com fontes até as regras de transformação, em um simples arquivo `config.yaml`.
- **Processadores Integrados**:
  - `json_parser`: Decodifica payloads JSON.
  - `rename_field`: Renomeia campos para adequação ao esquema de destino.
  - `regex_replace`: Mascaramento e transformação de dados sensíveis (suporta campos aninhados).
  - `filter`: Filtragem de registros baseada em condições lógicas.
- **Resiliência**: Gerenciamento de workers e timeouts de batch configuráveis.

## 🛠️ Arquitetura

O fluxo de dados no Go-Refinery segue o padrão **Source -> Processors -> Sink**:

```mermaid
graph LR
    A[Source (Kafka)] --> B(Engine)
    subgraph Pipeline
    B --> C{Processors}
    C --> D[JSON Parser]
    D --> E[Rename Field]
    E --> F[Regex Replace]
    F --> G[Filter]
    end
    G --> H[Sink (SQL Server)]
```

## 📋 Pré-requisitos

- **Go** 1.24+
- **Docker** e **Docker Compose** (para rodar dependências como Kafka e SQL Server)

## 📦 Instalação

1. Clone o repositório:
   ```bash
   git clone https://github.com/seu-usuario/go-refinery.git
   cd go-refinery
   ```

2. Baixe as dependências:
   ```bash
   go mod download
   ```

## ⚙️ Configuração

O comportamento do pipeline é controlado pelo arquivo `configs/config.yaml`.

### Exemplo de Configuração

```yaml
pipeline:
  worker_count: 200        # Número de workers paralelos
  batch_size: 1000         # Tamanho do lote para processamento
  batch_timeout: 1s        # Tempo máximo de espera para fechar um lote
  
  source:
    type: kafka
    config:
      brokers: ["localhost:9092"]
      topic: "orders"
      group_id: "order-processor"
  
  processors:
    - type: json_parser
    
    - type: rename_field
      config:
        mapping:
          "customer_id": "CustomerID"
          "total_amount": "Amount"

    - type: regex_replace
      config:
        field: "usuario.email"
        pattern: "(.*)@(.*)"
        replacement: "***@$2"

    - type: filter
      config:
        field: "Amount"
        operator: ">"
        value: 0.0

  sink:
    type: sqlserver
    config:
      dsn: "sqlserver://sa:Password@localhost:1433?database=mydb"
      table: "ProcessedOrders"
      fields:
        - source: "CustomerID"
          target: "customer_id"
        - source: "Amount"
          target: "total_value"
        - source: "usuario.email"
          target: "email_masked"
```

## ▶️ Como Rodar

### Localmente

1. Suba as dependências (Kafka, SQL Server) via Docker Compose:
   ```bash
   docker-compose up -d
   ```

2. Execute a aplicação:
   ```bash
   go run cmd/pipeline/main.go --config configs/config.yaml
   ```

### Via Docker

O projeto inclui um `Dockerfile` para facilitar o deploy.

1. Construa a imagem:
   ```bash
   docker build -t go-refinery .
   ```

2. Execute o container:
   ```bash
   docker run -v $(pwd)/configs:/app/configs go-refinery
   ```

## 📂 Estrutura do Projeto

```
go-refinery/
├── cmd/
│   └── pipeline/       # Ponto de entrada da aplicação (main.go)
├── configs/            # Arquivos de configuração
├── pkg/
│   ├── components/     # Implementações de Source, Sink e Processors
│   ├── config/         # Lógica de carregamento de configuração
│   └── pipeline/       # Motor principal do pipeline (Engine)
├── scripts/            # Scripts auxiliares
├── Dockerfile
├── docker-compose.yml
└── go.mod
```

## 🤝 Contribuindo

Contribuições são bem-vindas! Sinta-se à vontade para abrir issues ou enviar pull requests.

1. Faça um Fork do projeto
2. Crie sua Feature Branch (`git checkout -b feature/MinhaFeature`)
3. Commit suas mudanças (`git commit -m 'Adiciona MinhaFeature'`)
4. Push para a Branch (`git push origin feature/MinhaFeature`)
5. Abra um Pull Request

---

Desenvolvido por Robert Portilho
