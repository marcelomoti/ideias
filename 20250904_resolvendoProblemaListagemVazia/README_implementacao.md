# Lambda S3 File Mover - Implementação Otimizada

## Visão Geral

Esta solução foi desenvolvida especificamente para resolver os problemas que vocês estão enfrentando:

1. **Problema de listagem vazia do S3**: Implementa múltiplas estratégias de retry com backoff exponencial
2. **Alto volume (500k arquivos/dia)**: Otimizada para processamento em lotes com controle de concorrência
3. **Execução a cada 2 minutos**: Configurada para Step Functions com agendamento

## Características Principais

### 🔄 Estratégias Anti-Listagem Vazia
- **Retry com backoff exponencial**: 3 tentativas com intervalos crescentes
- **Múltiplas estratégias de listagem**: Padrão, paginação manual, filtro por tempo
- **Logging detalhado**: Para identificar padrões nos problemas

### ⚡ Otimizações de Performance
- **Processamento em lotes**: 100 arquivos por lote (configurável)
- **Limite de arquivos por execução**: 1000 arquivos (configurável)
- **Controle de concorrência**: Reserved concurrency para evitar throttling
- **Retry inteligente**: Apenas para erros recuperáveis

### 💰 Otimizações de Custo
- **Memória otimizada**: 512MB (ajustável conforme necessário)
- **Timeout controlado**: 15 minutos máximo
- **Processamento eficiente**: Minimiza tempo de execução

## Arquitetura Recomendada

```
EventBridge (a cada 2 min) → Step Functions → Lambda → S3 Operations
```

### Por que Step Functions?
1. **Retry automático**: Para falhas temporárias
2. **Monitoramento visual**: Acompanhar execuções
3. **Controle de fluxo**: Parar execuções em caso de erro crítico
4. **Custo baixo**: ~$0.60/mês para 500k execuções

## Configuração e Deploy

### 1. Preparação do Ambiente
```bash
# Instalar dependências
pip install -r requirements.txt

# Configurar AWS CLI
aws configure
```

### 2. Deploy com SAM
```bash
# Build
sam build

# Deploy
sam deploy --guided --parameter-overrides \
  SourceBucket=meu-bucket-origem \
  DestBucket=meu-bucket-destino \
  SourcePrefix=pasta-origem/ \
  DestPrefix=pasta-destino/
```

### 3. Deploy com Terraform (alternativo)
```hcl
resource "aws_lambda_function" "s3_file_mover" {
  filename         = "s3_file_mover_lambda.zip"
  function_name    = "s3-file-mover-lambda"
  role            = aws_iam_role.lambda_role.arn
  handler         = "s3_file_mover_lambda.lambda_handler"
  runtime         = "python3.9"
  timeout         = 900
  memory_size     = 512
  
  environment {
    variables = {
      SOURCE_BUCKET = var.source_bucket
      DEST_BUCKET   = var.dest_bucket
      SOURCE_PREFIX = var.source_prefix
      DEST_PREFIX   = var.dest_prefix
    }
  }
}
```

## Parâmetros de Configuração

### Environment Variables
- `SOURCE_BUCKET`: Bucket de origem (obrigatório)
- `DEST_BUCKET`: Bucket de destino (obrigatório)
- `SOURCE_PREFIX`: Pasta de origem (opcional)
- `DEST_PREFIX`: Pasta de destino (opcional)

### Event Parameters (Step Functions)
```json
{
  "source_bucket": "meu-bucket-origem",
  "dest_bucket": "meu-bucket-destino",
  "source_prefix": "pasta-origem/",
  "dest_prefix": "pasta-destino/",
  "max_files_per_execution": 1000,
  "batch_size": 100
}
```

## Monitoramento e Troubleshooting

### CloudWatch Metrics Importantes
- `Duration`: Tempo de execução
- `Errors`: Número de erros
- `Throttles`: Execuções limitadas
- `ConcurrentExecutions`: Execuções simultâneas

### Logs para Investigar
```bash
# Ver logs da Lambda
aws logs tail /aws/lambda/s3-file-mover-lambda --follow

# Ver logs da Step Function
aws logs tail /aws/stepfunctions/s3-file-mover-workflow --follow
```

### Troubleshooting Listagem Vazia

1. **Verificar permissões IAM**:
```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Action": [
        "s3:ListBucket",
        "s3:GetObject",
        "s3:PutObject",
        "s3:DeleteObject"
      ],
      "Resource": [
        "arn:aws:s3:::bucket-origem",
        "arn:aws:s3:::bucket-origem/*",
        "arn:aws:s3:::bucket-destino",
        "arn:aws:s3:::bucket-destino/*"
      ]
    }
  ]
}
```

2. **Verificar consistência eventual do S3**:
   - O código implementa retry automático
   - Logs mostrarão tentativas de listagem

3. **Verificar rate limiting**:
   - Implementado backoff exponencial
   - Pausas entre lotes para evitar throttling

## Estimativa de Custos (500k arquivos/dia)

### Lambda
- **Invocações**: ~720/mês (a cada 2 min) = $0.14
- **Duração**: 720 × 30s × 512MB = $3.60
- **Total Lambda**: ~$3.74/mês

### Step Functions
- **State transitions**: 720 × 3 estados = 2,160/mês = $0.05
- **Total Step Functions**: ~$0.05/mês

### S3 Operations
- **PUT requests**: 500k × 30 = 15M/mês = $7.50
- **DELETE requests**: 500k × 30 = 15M/mês = $6.00
- **LIST requests**: 720/mês = $0.00 (dentro do free tier)
- **Total S3**: ~$13.50/mês

### **Custo Total Estimado: ~$17.29/mês**

## Próximos Passos

1. **Deploy inicial**: Usar os arquivos fornecidos
2. **Teste com volume baixo**: Validar funcionamento
3. **Ajuste de parâmetros**: Otimizar batch_size e memory_size
4. **Monitoramento**: Configurar alarmes CloudWatch
5. **Scale up**: Aumentar volume gradualmente

## Suporte e Manutenção

### Ajustes Recomendados por Volume
- **< 100k arquivos/dia**: batch_size=50, memory=256MB
- **100k-500k arquivos/dia**: batch_size=100, memory=512MB
- **> 500k arquivos/dia**: batch_size=200, memory=1024MB

### Alertas Recomendados
- Error rate > 5%
- Duration > 10 minutos
- Throttles > 0
- Step Function failures

Precisa de ajuda com algum aspecto específico da implementação?
