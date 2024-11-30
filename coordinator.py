import argparse
import argparse
import asyncio
import aiohttp
from typing import Dict

class Coordinator:
    """Manages workers and aggregates results"""

    def __init__(self, port: int):
        print(f"Starting coordinator on port {port}")
        self.workers = {}  # Worker dictionary (id -> port)
        self.results = {}
        self.port = port

    def start(self) -> None:
        """Start coordinator server"""
        print(f"Starting coordinator on port {self.port}...")
        asyncio.run(self.listen_for_workers())

    async def listen_for_workers(self) -> None:
        while True:
            print("Listening for workers...")
            await asyncio.sleep(5)

    def register_worker(self, worker_id: str, worker_port: int) -> None:
        """Register new worker to the coordinator"""
        self.workers[worker_id] = worker_port
        print(f"Worker {worker_id} registered on port {worker_port}")

    async def distribute_work(self, filepath: str) -> None:
        """Split file and assign chunks to workers"""
        file_size = 1024  # Example file size, replace with actual file size logic
        chunk_size = 256
        num_chunks = file_size // chunk_size
        worker_count = len(self.workers)

        if worker_count == 0:
            print("No workers available to process the file.")
            return
        
        chunk_assignments = {}
        for i, (worker_id, port) in enumerate(self.workers.items()):
            chunk_assignments[worker_id] = {
                "start": i * chunk_size,
                "size": chunk_size
            }

        print(f"Distributing work: {chunk_assignments}")
        for worker_id, chunk_info in chunk_assignments.items():
            await self.send_chunk_to_worker(worker_id, filepath, chunk_info)

    async def send_chunk_to_worker(self, worker_id: str, filepath: str, chunk_info: Dict) -> None:
        worker_port = self.workers[worker_id]
        url = f"http://localhost:{worker_port}/process_chunk"
        params = {
            "filepath": filepath,
            "start": chunk_info["start"],
            "size": chunk_info["size"]
        }
        try:
            async with aiohttp.ClientSession() as session:
                async with session.post(url, json=params) as response:
                    print(f"Sent chunk to worker {worker_id}: {response.status}")
                    if response.status == 200:
                        worker_results = await response.json()
                        self.aggregate_results(worker_results)
        except Exception as e:
            print(f"Failed to send chunk to worker {worker_id}: {e}")
            await self.handle_worker_failure(worker_id)

    def aggregate_results(self, worker_results: Dict) -> None:
        for key, value in worker_results.items():
            if key not in self.results:
                self.results[key] = 0
            self.results[key] += value
        print(f"Current aggregated results: {self.results}")

    async def handle_worker_failure(self, worker_id: str) -> None:
        """Reassign work from failed worker"""
        print(f"Handling failure of worker {worker_id}...")
        # Logic to reassign work or handle worker failure

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Log Analyzer Coordinator")
    parser.add_argument("--port", type=int, default=8000, help="Coordinator port")
    args = parser.parse_args()

    coordinator = Coordinator(port=args.port)
    coordinator.start()