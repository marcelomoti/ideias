import json
import boto3
import os
import logging
from datetime import datetime, timedelta
from botocore.exceptions import ClientError, BotoCoreError
import time
import random

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
    Lambda function para mover arquivos entre buckets S3
    Otimizado para alto volume (500k arquivos/dia) com retry logic
    """
    
    try:
        source_bucket = event.get('source_bucket') or os.environ.get('SOURCE_BUCKET')
        dest_bucket = event.get('dest_bucket') or os.environ.get('DEST_BUCKET')
        source_prefix = event.get('source_prefix', '') or os.environ.get('SOURCE_PREFIX', '')
        dest_prefix = event.get('dest_prefix', '') or os.environ.get('DEST_PREFIX', '')
        
        max_files_per_execution = int(event.get('max_files_per_execution', 1000))
        batch_size = int(event.get('batch_size', 100))
        
        if not source_bucket or not dest_bucket:
            raise ValueError("source_bucket e dest_bucket são obrigatórios")
        
        logger.info(f"Iniciando movimentação: {source_bucket}/{source_prefix} -> {dest_bucket}/{dest_prefix}")
        
        files_to_process = get_files_with_retry(source_bucket, source_prefix, max_files_per_execution)
        
        if not files_to_process:
            logger.info("Nenhum arquivo encontrado para processar")
            return {
                'statusCode': 200,
                'body': json.dumps({
                    'message': 'Nenhum arquivo encontrado',
                    'files_processed': 0,
                    'source_bucket': source_bucket,
                    'dest_bucket': dest_bucket
                })
            }
        
        results = process_files_in_batches(
            files_to_process, 
            source_bucket, 
            dest_bucket, 
            source_prefix, 
            dest_prefix, 
            batch_size
        )
        
        logger.info(f"Processamento concluído: {results['success_count']} sucessos, {results['error_count']} erros")
        
        return {
            'statusCode': 200,
            'body': json.dumps({
                'message': 'Processamento concluído',
                'files_processed': results['success_count'],
                'errors': results['error_count'],
                'source_bucket': source_bucket,
                'dest_bucket': dest_bucket,
                'execution_time_seconds': results['execution_time']
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

def get_files_with_retry(bucket, prefix, max_files, max_retries=3):
    """
    Lista arquivos com retry para contornar problema de listagem vazia
    Implementa backoff exponencial e múltiplas estratégias
    """
    
    for attempt in range(max_retries):
        try:
            logger.info(f"Tentativa {attempt + 1} de listagem no bucket {bucket}")
            
            files = list_files_standard(bucket, prefix, max_files)
            
            if files:
                logger.info(f"Encontrados {len(files)} arquivos na tentativa {attempt + 1}")
                return files
            
            if attempt == 0:
                files = list_files_with_pagination(bucket, prefix, max_files)
                if files:
                    logger.info(f"Encontrados {len(files)} arquivos com paginação manual")
                    return files
            
            if attempt == 1:
                files = list_files_by_time_range(bucket, prefix, max_files)
                if files:
                    logger.info(f"Encontrados {len(files)} arquivos por range de tempo")
                    return files
            
            if attempt < max_retries - 1:
                wait_time = (2 ** attempt) + random.uniform(0, 1)
                logger.info(f"Aguardando {wait_time:.2f}s antes da próxima tentativa")
                time.sleep(wait_time)
                
        except ClientError as e:
            error_code = e.response['Error']['Code']
            logger.error(f"Erro AWS na tentativa {attempt + 1}: {error_code} - {str(e)}")
            
            if error_code in ['NoSuchBucket', 'AccessDenied']:
                raise  # Erros que não devem ser retentados
                
            if attempt < max_retries - 1:
                wait_time = (2 ** attempt) + random.uniform(0, 1)
                time.sleep(wait_time)
        
        except Exception as e:
            logger.error(f"Erro inesperado na tentativa {attempt + 1}: {str(e)}")
            if attempt < max_retries - 1:
                time.sleep(2 ** attempt)
    
    logger.warning("Todas as tentativas de listagem falharam")
    return []

def list_files_standard(bucket, prefix, max_files):
    """Listagem padrão com list_objects_v2"""
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
                if not obj['Key'].endswith('/'):  # Ignorar "pastas"
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

def list_files_with_pagination(bucket, prefix, max_files):
    """Listagem com controle manual de paginação"""
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
            if not obj['Key'].endswith('/'):
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

def list_files_by_time_range(bucket, prefix, max_files):
    """Listagem focada em arquivos recentes (últimas 24h)"""
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
                    if not obj['Key'].endswith('/'):
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

def process_files_in_batches(files, source_bucket, dest_bucket, source_prefix, dest_prefix, batch_size):
    """
    Processa arquivos em lotes para otimizar performance
    """
    start_time = time.time()
    success_count = 0
    error_count = 0
    
    for i in range(0, len(files), batch_size):
        batch = files[i:i + batch_size]
        logger.info(f"Processando lote {i//batch_size + 1}: {len(batch)} arquivos")
        
        batch_results = process_batch(batch, source_bucket, dest_bucket, source_prefix, dest_prefix)
        success_count += batch_results['success']
        error_count += batch_results['errors']
        
        if i + batch_size < len(files):
            time.sleep(0.1)
    
    execution_time = time.time() - start_time
    
    return {
        'success_count': success_count,
        'error_count': error_count,
        'execution_time': round(execution_time, 2)
    }

def process_batch(batch, source_bucket, dest_bucket, source_prefix, dest_prefix):
    """Processa um lote de arquivos"""
    success = 0
    errors = 0
    
    for file_info in batch:
        try:
            source_key = file_info['Key']
            
            if source_prefix and source_key.startswith(source_prefix):
                relative_key = source_key[len(source_prefix):].lstrip('/')
            else:
                relative_key = source_key
            
            dest_key = f"{dest_prefix.rstrip('/')}/{relative_key}" if dest_prefix else relative_key
            
            if move_file(source_bucket, source_key, dest_bucket, dest_key):
                success += 1
                logger.debug(f"Arquivo movido: {source_key} -> {dest_key}")
            else:
                errors += 1
                
        except Exception as e:
            logger.error(f"Erro ao processar arquivo {file_info['Key']}: {str(e)}")
            errors += 1
    
    return {'success': success, 'errors': errors}

def move_file(source_bucket, source_key, dest_bucket, dest_key, max_retries=2):
    """
    Move um arquivo (copia e deleta) com retry logic
    """
    
    for attempt in range(max_retries):
        try:
            copy_source = {'Bucket': source_bucket, 'Key': source_key}
            
            s3_client.copy_object(
                CopySource=copy_source,
                Bucket=dest_bucket,
                Key=dest_key,
                MetadataDirective='COPY'
            )
            
            try:
                s3_client.head_object(Bucket=dest_bucket, Key=dest_key)
            except ClientError:
                logger.error(f"Falha na verificação da cópia: {dest_key}")
                if attempt < max_retries - 1:
                    continue
                return False
            
            s3_client.delete_object(Bucket=source_bucket, Key=source_key)
            
            return True
            
        except ClientError as e:
            error_code = e.response['Error']['Code']
            logger.error(f"Erro AWS na tentativa {attempt + 1} para {source_key}: {error_code}")
            
            if error_code in ['NoSuchKey', 'NoSuchBucket', 'AccessDenied']:
                return False
            
            if attempt < max_retries - 1:
                time.sleep(0.5 * (attempt + 1))
                
        except Exception as e:
            logger.error(f"Erro inesperado na tentativa {attempt + 1} para {source_key}: {str(e)}")
            if attempt < max_retries - 1:
                time.sleep(0.5 * (attempt + 1))
    
    return False
