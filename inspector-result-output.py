import boto3
import csv
import io
from datetime import datetime
from operator import itemgetter

# 初始化Boto3客户端
s3 = boto3.client('s3')
inspector = boto3.client('inspector')

def flatten_finding(finding):
    """展平finding对象到一个字典."""
    flat_finding = {
        'arn': finding.get('arn', 'N/A'),
        'severity': finding.get('severity', 'N/A'),
        'title': finding.get('title', 'N/A'),
        'description': finding.get('description', 'N/A'),
        'createdAt': finding.get('createdAt', 'N/A'),
    }
    for attribute in finding.get('attributes', []):
        key_name = attribute.get('key')
        flat_finding[key_name] = attribute.get('value', 'N/A')
    return flat_finding

def get_latest_run_arn():
    """获取最新的评估运行ARN."""
    response = inspector.list_assessment_runs()
    arns = response['assessmentRunArns']
    if not arns:
        return None
    details = inspector.describe_assessment_runs(assessmentRunArns=arns)
    latest_run = sorted(details['assessmentRuns'], key=itemgetter('completedAt'), reverse=True)[0]
    return latest_run['arn']

def get_findings(assessment_run_arn):
    """获取指定评估运行的所有发现."""
    findings_arns, next_token = [], None
    while True:
        response = inspector.list_findings(assessmentRunArns=[assessment_run_arn], nextToken=next_token) if next_token else inspector.list_findings(assessmentRunArns=[assessment_run_arn])
        findings_arns.extend(response['findingArns'])
        next_token = response.get('nextToken')
        if not next_token:
            break
    return [inspector.describe_findings(findingArns=[arn])['findings'][0] for arn in findings_arns]

def generate_csv(findings):
    """从发现生成CSV内容."""
    output = io.StringIO()
    writer = csv.writer(output)
    if findings:
        writer.writerow(flatten_finding(findings[0]).keys())  # 写入头部
        for finding in findings:
            writer.writerow(flatten_finding(finding).values())
    return output.getvalue()

def lambda_handler(event, context):
    try:
        latest_run_arn = get_latest_run_arn()
        if not latest_run_arn:
            return {'statusCode': 400, 'body': 'No assessment runs found.'}
        
        findings = get_findings(latest_run_arn)
        csv_content = generate_csv(findings)
        
        timestamp = datetime.now().strftime('%Y%m%d%H%M%S')
        bucket_name = 'lambda-to-s3-uptrillionuat'
        object_key = f'detailed-inspection-report-{timestamp}.csv'
        
        s3.put_object(Bucket=bucket_name, Key=object_key, Body=csv_content)
        return {'statusCode': 200, 'body': f'Detailed CSV report successfully saved to S3 with key {object_key}.'}
    except Exception as e:
        print(f"Error occurred: {str(e)}")
        return {'statusCode': 500, 'body': f'An error occurred: {str(e)}'}
