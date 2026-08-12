from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator

default_args = {
    'owner': 'cryptopulse',
    'depends_on_past': False,
    'start_date': datetime(2026, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

def run_spark_retrain():
    import json
    import socket
    import http.client
    import subprocess

    cmd = ["/opt/spark/bin/spark-submit", "/app/spark/retrain_model.py"]

    # 1. Try Docker SDK
    try:
        import docker
        client = docker.DockerClient(base_url='unix://var/run/docker.sock')
        container = client.containers.get('cryptopulse-spark')
        res = container.exec_run(cmd)
        output = res.output.decode('utf-8', errors='ignore')
        print(output)
        if res.exit_code == 0:
            print("✅ Retrained GBT model successfully via Docker SDK!")
            return
        else:
            raise Exception(f"Spark retrain failed (exit code {res.exit_code}): {output}")
    except Exception as e:
        print(f"⚠️ Docker SDK method exception: {e}")

    # 2. Try Unix HTTP socket API directly
    try:
        class UnixHTTPConnection(http.client.HTTPConnection):
            def __init__(self, path):
                super().__init__('localhost')
                self.path = path
            def connect(self):
                self.sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
                self.sock.connect(self.path)

        conn = UnixHTTPConnection('/var/run/docker.sock')
        payload = json.dumps({"AttachStdout": True, "AttachStderr": True, "Cmd": cmd})
        conn.request("POST", "/containers/cryptopulse-spark/exec", payload, {"Content-Type": "application/json"})
        resp = conn.getresponse()
        data = json.loads(resp.read().decode())
        exec_id = data["Id"]

        conn.request("POST", f"/exec/{exec_id}/start", json.dumps({"Detach": False, "Tty": False}), {"Content-Type": "application/json"})
        resp = conn.getresponse()
        output = resp.read().decode('utf-8', errors='ignore')
        print(output)
        print("✅ Retrained GBT model successfully via Unix Socket HTTP API!")
        return
    except Exception as e:
        print(f"❌ Unix HTTP socket method exception: {e}")
        raise RuntimeError(f"Failed to execute spark retrain command: {e}")


with DAG(
    dag_id='crypto_gbt_retrain_dag',
    default_args=default_args,
    description='Weekly PySpark GBT Model Retraining Pipeline',
    schedule='0 0 * * 0',
    catchup=False,
) as dag:

    retrain_gbt_model = PythonOperator(
        task_id='retrain_pyspark_gbt',
        python_callable=run_spark_retrain,
    )

    retrain_gbt_model

    