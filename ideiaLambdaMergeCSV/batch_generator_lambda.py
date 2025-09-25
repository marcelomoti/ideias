import json
import boto3
import os
import logging
import math

logger = logging.getLogger()
logger.setLevel(logging.INFO)

s3_client = boto3.client('s3')

def lambda_handler(event, context):
    """
    Lambda para gerar lista de lotes para processamento paralelo
    Divide os arquivos CSV em lotes de 1000 para processamento otimizado
    """
    
    try:
        source_bucket = event.get('source_bucket')
        source_prefix = event.get('source_prefix', '')
        total_files = event.get('total_files', 300000)
        files_per_batch = event.get('files_per_batch', 1000)
        
        if not source_bucket:
            raise ValueError("source_bucket é obrigatório")
        
        logger.info(f"Gerando lotes para {total_files} arquivos em lotes de {files_per_batch}")
        
        total_batches = math.ceil(total_files / files_per_batch)
        batches = []
        
        for batch_num in range(total_batches):
            start_index = batch_num * files_per_batch
            end_index = min(start_index + files_per_batch, total_files)
            
            batch_info = {
                "batch_number": batch_num + 1,
                "batch_start_index": start_index,
                "batch_end_index": end_index,
                "source_bucket": source_bucket,
                "source_prefix": source_prefix,
                "output_filename": f"merged_batch_{batch_num + 1:03d}.csv"
            }
            
            batches.append(batch_info)
        
        logger.info(f"Gerados {len(batches)} lotes para processamento")
        
        return {
            'statusCode': 200,
            'batches': batches,
            'total_batches': len(batches),
            'total_files': total_files,
            'files_per_batch': files_per_batch
        }
        
    except Exception as e:
        logger.error(f"Erro na geração de lotes: {str(e)}")
        return {
            'statusCode': 500,
            'error': str(e),
            'message': 'Erro na geração de lotes'
        }
