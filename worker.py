import argparse
import time
import asyncio
import requests
from typing import Dict


class Worker:
    """Processes log chunks and reports results"""
    
    def __init__(self, port: int, worker_id: str, coordinator_url: str):
        self.worker_id = worker_id
        self.coordinator_url = coordinator_url
        self.port = port
    
    def start(self) -> None:
        """Start worker server"""
        print(f"Starting worker {self.worker_id} on port {self.port}...")
        asyncio.run(self.report_health())

    async def process_chunk(self, filepath: str, start: int, size: int) -> dict:
        """Process a chunk of log file and return metrics"""
        log_data = """
        2024-01-24 10:15:32.123 INFO Request processed in 127ms
        2024-01-24 10:15:33.001 ERROR Database connection failed
        2024-01-24 10:15:34.042 INFO Request processed in 95ms
        2024-01-24 10:15:35.567 INFO Request processed in 150ms
        2024-01-24 10:15:36.789 ERROR Timeout occurred
        """
        results = {"errors": 0, "requests": 0, "total_time": 0}
        
        # Simulate processing the "chunk" (this would be an actual file chunk in a real scenario)
        for line in log_data.splitlines():
            if "ERROR" in line:
                results["errors"] += 1
            if "processed in" in line:
                results["requests"] += 1
                results["total_time"] += int(line.split("in")[1].strip("ms"))
        
        return results

    async def report_health(self) -> None:
        """Send heartbeat to coordinator"""
        while True:
            try:
                response = requests.post(
                    f"{self.coordinator_url}/health",
                    json={"worker_id": self.worker_id}
                )
                print(f"Health reported: {response.status_code}")
            except Exception as e:
                print(f"Failed to report health: {e}")
            await asyncio.sleep(5)

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Log Analyzer Coordinator")
    parser.add_argument("--port", type=int, default=8000, help="Coordinator port")
    parser.add_argument("--id", type=str, default="worker1", help="Worker ID")
    parser.add_argument("--coordinator", type=str, default="http://localhost:8000", help="Coordinator URL")
    args = parser.parse_args()

    worker = Worker(port=args.port, worker_id=args.id, coordinator_url=args.coordinator)
    worker.start()
