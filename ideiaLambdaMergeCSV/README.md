# Lambda CSV Merge - Solução Paralela para 300k Arquivos

## Visão Geral

Esta solução foi desenvolvida para resolver o problema de performance no merge de 300.000 arquivos CSV do S3, reduzindo o tempo de processamento de 5 horas para aproximadamente 30-60 minutos através de processamento paralelo.

### Problema Original
- **Volume**: 300.000 arquivos CSV no S3
- **Formato**: Cada CSV tem 1 linha de header + 1 linha de dados
- **Performance atual**: 10 minutos para 1000 arquivos = 5 horas total
- **Limitação**: Processamento sequencial

### Solução Implementada
- **Arquitetura**: AWS Step Functions + Lambda paralelo
- **Paralelismo**: Até 50 execuções simultâneas
- **Lotes**: 1000 arquivos por Lambda (300 lotes total)
- **Tempo estimado**: 30-60 minutos (redução de 80-90%)

## Arquitetura

```
EventBridge/Manual → Step Functions → Batch Generator Lambda
                                   ↓
                    Map State (50 execuções paralelas)
                                   ↓
                    CSV Merge Lambda (300 instâncias)
                                   ↓
                    Results Aggregator Lambda
```

### Componentes

1. **csv_merge_lambda.py**: Lambda principal que faz merge de até 1000 CSVs
2. **batch_generator_lambda.py**: Gera lista de lotes para processamento paralelo
3. **results_aggregator_lambda.py**: Agrega resultados e gera relatório final
4. **step_function_parallel_csv_merge.json**: Orquestração do workflow paralelo

## Funcionalidades

### CSV Merge Lambda
- ✅ Lista arquivos CSV com retry logic (resolve problema de listagem vazia)
- ✅ Remove headers duplicados automaticamente
- ✅ Processamento paralelo interno com ThreadPoolExecutor
- ✅ Merge otimizado em memória
- ✅ Retry automático para falhas temporárias
- ✅ Logs detalhados para monitoramento

### Batch Generator
- ✅ Divide 300k arquivos em 300 lotes de 1000
- ✅ Gera configuração para cada lote
- ✅ Nomeação automática dos arquivos de saída

### Results Aggregator
- ✅ Coleta resultados de todas as execuções paralelas
- ✅ Gera relatório consolidado
- ✅ Estatísticas de performance
- ✅ Lista de arquivos gerados
- ✅ Tratamento de erros

## Configuração e Deploy

### 1. Pré-requisitos
```bash
# AWS CLI configurado
aws configure list

# SAM CLI instalado
sam --version
```

### 2. Deploy com SAM
```bash
# Build
sam build

# Deploy
sam deploy --guided --parameter-overrides \
  SourceBucket=meu-bucket-origem \
  DestBucket=meu-bucket-destino \
  SourcePrefix=csvs/ \
  DestPrefix=merged-csvs/
```

### 3. Execução Manual
```bash
# Executar Step Function
aws stepfunctions start-execution \
  --state-machine-arn arn:aws:states:region:account:stateMachine:csv-parallel-merge-workflow \
  --input '{
    "source_bucket": "meu-bucket-origem",
    "dest_bucket": "meu-bucket-destino", 
    "source_prefix": "csvs/",
    "dest_prefix": "merged-csvs/"
  }'
```

## Performance e Custos

### Estimativa de Performance
- **Execuções paralelas**: 50 simultâneas
- **Tempo por lote**: 2-4 minutos (1000 arquivos)
- **Tempo total**: 30-60 minutos
- **Melhoria**: 80-90% mais rápido

### Estimativa de Custos (us-east-1)
- **Lambda invocations**: 300 execuções principais + overhead = ~$0.06
- **Lambda duration**: 300 × 3min × 1024MB = ~$3.00
- **Step Functions**: 300 state transitions = ~$0.01
- **S3 requests**: 300k GET + 300 PUT = ~$150
- **Total estimado**: ~$153/execução

### Otimizações de Custo
- Use S3 Intelligent Tiering para arquivos antigos
- Configure lifecycle policies
- Use Reserved Capacity para execuções frequentes

## Monitoramento

### CloudWatch Metrics
- Lambda duration, errors, throttles
- Step Functions execution status
- S3 request metrics

### Logs Importantes
```bash
# Logs da Step Function
aws logs tail /aws/stepfunctions/csv-parallel-merge-workflow --follow

# Logs do CSV Merge Lambda
aws logs tail /aws/lambda/csv-merge-lambda --follow

# Logs do Batch Generator
aws logs tail /aws/lambda/csv-batch-generator-lambda --follow
```

### Alarmes Recomendados
- Lambda errors > 5%
- Step Function failed executions
- Lambda throttling
- Execution duration > 90 minutos

## Troubleshooting

### Problemas Comuns

1. **Listagem S3 vazia**
   - Implementado retry com backoff exponencial
   - Múltiplas estratégias de listagem
   - Logs detalhados para debug

2. **Lambda timeout**
   - Configurado para 15 minutos (máximo)
   - Processamento em lotes menores se necessário
   - Monitoramento de duration

3. **Throttling**
   - Reserved concurrency configurado
   - Backoff automático
   - Retry logic implementado

4. **Memória insuficiente**
   - Configurado 1024MB para merge Lambda
   - Processamento streaming quando possível
   - Monitoramento de memory usage

### Debug Steps
```bash
# Verificar execução da Step Function
aws stepfunctions describe-execution --execution-arn <arn>

# Verificar logs específicos
aws logs filter-log-events \
  --log-group-name /aws/lambda/csv-merge-lambda \
  --filter-pattern "ERROR"

# Verificar métricas
aws cloudwatch get-metric-statistics \
  --namespace AWS/Lambda \
  --metric-name Duration \
  --dimensions Name=FunctionName,Value=csv-merge-lambda
```

## Limitações e Considerações

### Limitações Técnicas
- **Lambda timeout**: 15 minutos máximo
- **Lambda memory**: 10GB máximo
- **Step Functions**: 25k state transitions máximo
- **S3 list**: 1000 objetos por página

### Considerações de Escala
- Para > 500k arquivos: considere usar AWS Batch
- Para arquivos > 10MB: ajuste memory e timeout
- Para execuções frequentes: considere provisioned concurrency

### Segurança
- IAM roles com permissões mínimas
- Encryption em trânsito e repouso
- VPC endpoints se necessário
- CloudTrail para auditoria

## Próximos Passos

1. **Teste com dados reais**: Validar com subset dos 300k arquivos
2. **Ajuste de performance**: Otimizar batch_size e concurrency
3. **Monitoramento**: Configurar alarmes e dashboards
4. **Automação**: Integrar com EventBridge para execução automática
5. **Backup**: Implementar estratégia de backup dos CSVs merged

## Suporte

Para dúvidas ou problemas:
1. Verificar logs do CloudWatch
2. Consultar métricas de performance
3. Revisar configurações de IAM
4. Testar com volumes menores primeiro
