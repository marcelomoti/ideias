import unittest
import json
import io
import boto3
from moto import mock_s3
from csv_merge_lambda import lambda_handler, read_csv_from_s3, merge_csv_files

class TestCSVMergeLambda(unittest.TestCase):
    
    @mock_s3
    def setUp(self):
        """Setup test environment with mock S3"""
        self.s3_client = boto3.client('s3', region_name='us-east-1')
        self.source_bucket = 'test-source-bucket'
        self.dest_bucket = 'test-dest-bucket'
        
        self.s3_client.create_bucket(Bucket=self.source_bucket)
        self.s3_client.create_bucket(Bucket=self.dest_bucket)
        
        self.create_test_csv_files()
    
    def create_test_csv_files(self):
        """Create sample CSV files for testing"""
        csv_files = [
            {
                'key': 'test1.csv',
                'content': 'name,age,city\nJoao,25,Sao Paulo'
            },
            {
                'key': 'test2.csv', 
                'content': 'name,age,city\nMaria,30,Rio de Janeiro'
            },
            {
                'key': 'test3.csv',
                'content': 'name,age,city\nPedro,35,Belo Horizonte'
            }
        ]
        
        for csv_file in csv_files:
            self.s3_client.put_object(
                Bucket=self.source_bucket,
                Key=csv_file['key'],
                Body=csv_file['content'].encode('utf-8'),
                ContentType='text/csv'
            )
    
    @mock_s3
    def test_read_csv_from_s3(self):
        """Test reading CSV content from S3"""
        content = read_csv_from_s3(self.source_bucket, 'test1.csv')
        self.assertIsNotNone(content)
        if content:
            self.assertIn('name,age,city', content)
            self.assertIn('Joao,25,Sao Paulo', content)
    
    @mock_s3
    def test_lambda_handler_success(self):
        """Test successful Lambda execution"""
        event = {
            'source_bucket': self.source_bucket,
            'dest_bucket': self.dest_bucket,
            'source_prefix': '',
            'dest_prefix': 'merged',
            'max_files_per_execution': 10,
            'batch_size': 2,
            'output_filename': 'test_merged.csv'
        }
        
        context = {}
        
        response = lambda_handler(event, context)
        
        self.assertEqual(response['statusCode'], 200)
        
        body = json.loads(response['body'])
        self.assertEqual(body['files_processed'], 3)
        self.assertIn('test_merged.csv', body['output_file'])
        
        merged_objects = self.s3_client.list_objects_v2(
            Bucket=self.dest_bucket,
            Prefix='merged/'
        )
        self.assertIn('Contents', merged_objects)
        self.assertEqual(len(merged_objects['Contents']), 1)
        
        merged_obj = self.s3_client.get_object(
            Bucket=self.dest_bucket,
            Key='merged/test_merged.csv'
        )
        merged_content = merged_obj['Body'].read().decode('utf-8')
        
        lines = merged_content.strip().split('\n')
        self.assertEqual(len(lines), 4)  # 1 header + 3 data lines
        self.assertEqual(lines[0], 'name,age,city')  # Header only once
        self.assertIn('Joao,25,Sao Paulo', merged_content)
        self.assertIn('Maria,30,Rio de Janeiro', merged_content)
        self.assertIn('Pedro,35,Belo Horizonte', merged_content)
    
    @mock_s3
    def test_lambda_handler_no_files(self):
        """Test Lambda execution with no CSV files"""
        objects = self.s3_client.list_objects_v2(Bucket=self.source_bucket)
        if 'Contents' in objects:
            for obj in objects['Contents']:
                self.s3_client.delete_object(
                    Bucket=self.source_bucket,
                    Key=obj['Key']
                )
        
        event = {
            'source_bucket': self.source_bucket,
            'dest_bucket': self.dest_bucket,
            'source_prefix': '',
            'dest_prefix': 'merged',
            'max_files_per_execution': 10,
            'batch_size': 2
        }
        
        context = {}
        
        response = lambda_handler(event, context)
        
        self.assertEqual(response['statusCode'], 200)
        body = json.loads(response['body'])
        self.assertEqual(body['files_processed'], 0)
        self.assertEqual(body['message'], 'Nenhum arquivo CSV encontrado')
    
    @mock_s3
    def test_lambda_handler_missing_bucket(self):
        """Test Lambda execution with missing bucket parameter"""
        event = {
            'dest_bucket': self.dest_bucket,
            'source_prefix': '',
            'dest_prefix': 'merged'
        }
        
        context = {}
        
        response = lambda_handler(event, context)
        
        self.assertEqual(response['statusCode'], 500)
        body = json.loads(response['body'])
        self.assertIn('source_bucket e dest_bucket são obrigatórios', body['error'])

if __name__ == '__main__':
    unittest.main()
