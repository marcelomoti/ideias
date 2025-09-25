import json
import boto3
import os
import logging
import csv
import io
from datetime import datetime, timedelta
from botocore.exceptions import ClientError, BotoCoreError
import time
import random
from concurrent.futures import ThreadPoolExecutor, as_completed
import threading

logger = logging.getLogger()
logger.setLevel(logging.INFO)

s3_client = boto3.client('s3', 
    config=boto3.session.Config(
        retries={'max_attempts': 3, 'mode': 'adaptive'},
        max_pool_connections=50
    )
)

def lambda_handler(event, context):
    """
    Lambda function para fazer merge de arquivos CSV do S3
    Processa até 1000 arquivos CSV por execução, removendo headers duplicados
    """
    
    try:
        source_bucket = event.get('source_bucket') or os.environ.get('SOURCE_BUCKET')
        dest_bucket = event.get('dest_bucket') or os.environ.get('DEST_BUCKET')
        source_prefix = event.get('source_prefix', '') or os.environ.get('SOURCE_PREFIX', '')
        dest_prefix = event.get('dest_prefix', '') or os.environ.get('DEST_PREFIX', '')
        
        max_files_per_execution = int(event.get('max_files_per_execution', 1000))
        batch_size = int(event.get('batch_size', 100))
        output_filename = event.get('output_filename') or f"merged_csv_{int(time.time())}.csv"
        
        if not source_bucket or not dest_bucket:
            raise ValueError("source_bucket e dest_bucket são obrigatórios")
        
        logger.info(f"Iniciando merge de CSVs: {source_bucket}/{source_prefix} -> {dest_bucket}/{dest_prefix}")
        logger.info(f"Arquivo de saída: {output_filename}")
        
        csv_files = get_csv_files_with_retry(source_bucket, source_prefix, max_files_per_execution)
        
        if not csv_files:
            logger.info("Nenhum arquivo CSV encontrado para processar")
            return {
                'statusCode': 200,
                'body': json.dumps({
                    'message': 'Nenhum arquivo CSV encontrado',
                    'files_processed': 0,
                    'source_bucket': source_bucket,
                    'dest_bucket': dest_bucket
                })
            }
        
        results = merge_csv_files(
            csv_files, 
            source_bucket, 
            dest_bucket, 
            dest_prefix, 
            output_filename,
            batch_size
        )
        
        logger.info(f"Merge concluído: {results['files_processed']} arquivos processados")
        
        return {
            'statusCode': 200,
            'body': json.dumps({
                'message': 'Merge de CSVs concluído',
                'files_processed': results['files_processed'],
                'output_file': results['output_file'],
                'source_bucket': source_bucket,
                'dest_bucket': dest_bucket,
                'execution_time_seconds': results['execution_time'],
                'total_rows': results['total_rows']
            })
        }
        
    except Exception as e:
        logger.error(f"Erro na execução da Lambda: {str(e)}")
        return {
            'statusCode': 500,
            'body': json.dumps({
                'error': str(e),
                'message': 'Erro na execução da Lambda'
            })
        }

def get_csv_files_with_retry(bucket, prefix, max_files, max_retries=3):
    """
    Lista arquivos CSV com retry para contornar problema de listagem vazia
    Implementa backoff exponencial e múltiplas estratégias
    """
    
    for attempt in range(max_retries):
        try:
            logger.info(f"Tentativa {attempt + 1} de listagem no bucket {bucket}")
            
            files = list_csv_files_standard(bucket, prefix, max_files)
            
            if files:
                logger.info(f"Encontrados {len(files)} arquivos CSV na tentativa {attempt + 1}")
                return files
            
            if attempt == 0:
                files = list_csv_files_with_pagination(bucket, prefix, max_files)
                if files:
                    logger.info(f"Encontrados {len(files)} arquivos CSV com paginação manual")
                    return files
            
            if attempt == 1:
                files = list_csv_files_by_time_range(bucket, prefix, max_files)
                if files:
                    logger.info(f"Encontrados {len(files)} arquivos CSV por range de tempo")
                    return files
            
            if attempt < max_retries - 1:
                wait_time = (2 ** attempt) + random.uniform(0, 1)
                logger.info(f"Aguardando {wait_time:.2f}s antes da próxima tentativa")
                time.sleep(wait_time)
                
        except ClientError as e:
            error_code = e.response['Error']['Code']
            logger.error(f"Erro AWS na tentativa {attempt + 1}: {error_code} - {str(e)}")
            
            if error_code in ['NoSuchBucket', 'AccessDenied']:
                raise
                
            if attempt < max_retries - 1:
                wait_time = (2 ** attempt) + random.uniform(0, 1)
                time.sleep(wait_time)
        
        except Exception as e:
            logger.error(f"Erro inesperado na tentativa {attempt + 1}: {str(e)}")
            if attempt < max_retries - 1:
                time.sleep(2 ** attempt)
    
    logger.warning("Todas as tentativas de listagem falharam")
    return []

def list_csv_files_standard(bucket, prefix, max_files):
    """Listagem padrão de arquivos CSV com list_objects_v2"""
    files = []
    paginator = s3_client.get_paginator('list_objects_v2')
    
    page_iterator = paginator.paginate(
        Bucket=bucket,
        Prefix=prefix,
        PaginationConfig={'MaxItems': max_files, 'PageSize': 1000}
    )
    
    for page in page_iterator:
        if 'Contents' in page:
            for obj in page['Contents']:
                if obj['Key'].lower().endswith('.csv') and not obj['Key'].endswith('/'):
                    files.append({
                        'Key': obj['Key'],
                        'Size': obj['Size'],
                        'LastModified': obj['LastModified']
                    })
                    
                    if len(files) >= max_files:
                        break
        
        if len(files) >= max_files:
            break
    
    return files

def list_csv_files_with_pagination(bucket, prefix, max_files):
    """Listagem de CSVs com controle manual de paginação"""
    files = []
    continuation_token = None
    
    while len(files) < max_files:
        params = {
            'Bucket': bucket,
            'Prefix': prefix,
            'MaxKeys': min(1000, max_files - len(files))
        }
        
        if continuation_token:
            params['ContinuationToken'] = continuation_token
        
        response = s3_client.list_objects_v2(**params)
        
        if 'Contents' not in response:
            break
        
        for obj in response['Contents']:
            if obj['Key'].lower().endswith('.csv') and not obj['Key'].endswith('/'):
                files.append({
                    'Key': obj['Key'],
                    'Size': obj['Size'],
                    'LastModified': obj['LastModified']
                })
        
        if not response.get('IsTruncated', False):
            break
            
        continuation_token = response.get('NextContinuationToken')
        time.sleep(0.1)
    
    return files

def list_csv_files_by_time_range(bucket, prefix, max_files):
    """Listagem de CSVs focada em arquivos recentes (últimas 24h)"""
    files = []
    cutoff_time = datetime.now() - timedelta(hours=24)
    
    try:
        paginator = s3_client.get_paginator('list_objects_v2')
        page_iterator = paginator.paginate(
            Bucket=bucket,
            Prefix=prefix,
            PaginationConfig={'MaxItems': max_files * 2, 'PageSize': 500}
        )
        
        for page in page_iterator:
            if 'Contents' in page:
                for obj in page['Contents']:
                    if obj['Key'].lower().endswith('.csv') and not obj['Key'].endswith('/'):
                        if obj['LastModified'].replace(tzinfo=None) >= cutoff_time:
                            files.append({
                                'Key': obj['Key'],
                                'Size': obj['Size'],
                                'LastModified': obj['LastModified']
                            })
                            
                            if len(files) >= max_files:
                                break
            
            if len(files) >= max_files:
                break
                
    except Exception as e:
        logger.error(f"Erro na listagem por tempo: {str(e)}")
    
    return files

def merge_csv_files(csv_files, source_bucket, dest_bucket, dest_prefix, output_filename, batch_size):
    """
    Faz merge de arquivos CSV removendo headers duplicados
    Usa processamento paralelo para otimizar performance
    """
    start_time = time.time()
    total_rows = 0
    files_processed = 0
    
    merged_content = io.StringIO()
    header_written = False
    
    lock = threading.Lock()
    
    def process_csv_batch(batch):
        nonlocal total_rows, files_processed, header_written
        batch_content = []
        batch_rows = 0
        
        for file_info in batch:
            try:
                csv_content = read_csv_from_s3(source_bucket, file_info['Key'])
                if csv_content:
                    lines = csv_content.strip().split('\n')
                    if len(lines) >= 2:
                        header = lines[0]
                        data_line = lines[1]
                        
                        with lock:
                            if not header_written:
                                batch_content.append(header)
                                header_written = True
                            batch_content.append(data_line)
                            batch_rows += 1
                            files_processed += 1
                        
                        logger.debug(f"Processado: {file_info['Key']}")
                    else:
                        logger.warning(f"Arquivo CSV inválido (menos de 2 linhas): {file_info['Key']}")
                        
            except Exception as e:
                logger.error(f"Erro ao processar {file_info['Key']}: {str(e)}")
        
        return batch_content, batch_rows
    
    with ThreadPoolExecutor(max_workers=min(10, len(csv_files))) as executor:
        futures = []
        
        for i in range(0, len(csv_files), batch_size):
            batch = csv_files[i:i + batch_size]
            future = executor.submit(process_csv_batch, batch)
            futures.append(future)
        
        for future in as_completed(futures):
            try:
                batch_content, batch_rows = future.result()
                for line in batch_content:
                    merged_content.write(line + '\n')
                total_rows += batch_rows
                
            except Exception as e:
                logger.error(f"Erro no processamento do batch: {str(e)}")
    
    output_key = f"{dest_prefix.rstrip('/')}/{output_filename}" if dest_prefix else output_filename
    
    try:
        merged_csv = merged_content.getvalue()
        s3_client.put_object(
            Bucket=dest_bucket,
            Key=output_key,
            Body=merged_csv.encode('utf-8'),
            ContentType='text/csv'
        )
        
        logger.info(f"Arquivo merged salvo: s3://{dest_bucket}/{output_key}")
        
    except Exception as e:
        logger.error(f"Erro ao salvar arquivo merged: {str(e)}")
        raise
    
    execution_time = time.time() - start_time
    
    return {
        'files_processed': files_processed,
        'output_file': f"s3://{dest_bucket}/{output_key}",
        'execution_time': round(execution_time, 2),
        'total_rows': total_rows
    }

def read_csv_from_s3(bucket, key, max_retries=2):
    """
    Lê conteúdo de um arquivo CSV do S3 com retry logic
    """
    
    for attempt in range(max_retries):
        try:
            response = s3_client.get_object(Bucket=bucket, Key=key)
            content = response['Body'].read().decode('utf-8')
            return content
            
        except ClientError as e:
            error_code = e.response['Error']['Code']
            logger.error(f"Erro AWS na tentativa {attempt + 1} para {key}: {error_code}")
            
            if error_code in ['NoSuchKey', 'NoSuchBucket', 'AccessDenied']:
                return None
            
            if attempt < max_retries - 1:
                time.sleep(0.5 * (attempt + 1))
                
        except Exception as e:
            logger.error(f"Erro inesperado na tentativa {attempt + 1} para {key}: {str(e)}")
            if attempt < max_retries - 1:
                time.sleep(0.5 * (attempt + 1))
    
    return None
