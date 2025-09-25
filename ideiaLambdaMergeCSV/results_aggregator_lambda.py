import json
import boto3
import os
import logging
from datetime import datetime

logger = logging.getLogger()
logger.setLevel(logging.INFO)

s3_client = boto3.client('s3')

def lambda_handler(event, context):
    """
    Lambda para agregar resultados do processamento paralelo
    Gera relatório final e estatísticas do merge de CSVs
    """
    
    try:
        parallel_results = event.get('parallel_results', [])
        dest_bucket = event.get('dest_bucket')
        dest_prefix = event.get('dest_prefix', '')
        
        if not dest_bucket:
            raise ValueError("dest_bucket é obrigatório")
        
        logger.info(f"Agregando resultados de {len(parallel_results)} execuções paralelas")
        
        total_files_processed = 0
        total_rows = 0
        successful_batches = 0
        failed_batches = 0
        execution_times = []
        output_files = []
        errors = []
        
        for result in parallel_results:
            try:
                if 'Payload' in result and 'body' in result['Payload']:
                    body = json.loads(result['Payload']['body'])
                    
                    if result['Payload']['statusCode'] == 200:
                        successful_batches += 1
                        total_files_processed += body.get('files_processed', 0)
                        total_rows += body.get('total_rows', 0)
                        execution_times.append(body.get('execution_time_seconds', 0))
                        
                        if 'output_file' in body:
                            output_files.append(body['output_file'])
                    else:
                        failed_batches += 1
                        errors.append({
                            'error': body.get('error', 'Erro desconhecido'),
                            'message': body.get('message', '')
                        })
                        
                elif 'error' in result:
                    failed_batches += 1
                    errors.append({
                        'error': str(result['error']),
                        'batch_info': result.get('batch_info', {})
                    })
                    
            except Exception as e:
                logger.error(f"Erro ao processar resultado: {str(e)}")
                failed_batches += 1
                errors.append({
                    'error': f"Erro no parsing do resultado: {str(e)}",
                    'raw_result': str(result)
                })
        
        avg_execution_time = sum(execution_times) / len(execution_times) if execution_times else 0
        
        summary_report = {
            'execution_summary': {
                'timestamp': datetime.now().isoformat(),
                'total_batches': len(parallel_results),
                'successful_batches': successful_batches,
                'failed_batches': failed_batches,
                'success_rate': (successful_batches / len(parallel_results)) * 100 if parallel_results else 0
            },
            'processing_stats': {
                'total_files_processed': total_files_processed,
                'total_data_rows': total_rows,
                'average_execution_time_seconds': round(avg_execution_time, 2),
                'total_execution_time_seconds': round(sum(execution_times), 2)
            },
            'output_files': output_files,
            'errors': errors[:10] if errors else []
        }
        
        report_key = f"{dest_prefix.rstrip('/')}/execution_report_{int(datetime.now().timestamp())}.json" if dest_prefix else f"execution_report_{int(datetime.now().timestamp())}.json"
        
        try:
            s3_client.put_object(
                Bucket=dest_bucket,
                Key=report_key,
                Body=json.dumps(summary_report, indent=2, ensure_ascii=False).encode('utf-8'),
                ContentType='application/json'
            )
            
            logger.info(f"Relatório salvo: s3://{dest_bucket}/{report_key}")
            
        except Exception as e:
            logger.error(f"Erro ao salvar relatório: {str(e)}")
        
        logger.info(f"Agregação concluída: {successful_batches}/{len(parallel_results)} lotes bem-sucedidos")
        
        return {
            'statusCode': 200,
            'summary': summary_report,
            'report_location': f"s3://{dest_bucket}/{report_key}"
        }
        
    except Exception as e:
        logger.error(f"Erro na agregação de resultados: {str(e)}")
        return {
            'statusCode': 500,
            'error': str(e),
            'message': 'Erro na agregação de resultados'
        }
