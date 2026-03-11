"""
Main arbitrage client that sends pool updates to all detector processes
via threads and collects the best result.

Usage:
    from arbitrage_client import ArbitrageClient

    client = ArbitrageClient.from_detectors_file("detectors.json")
    result = client.send_update(pool_json_str)

Or standalone test:
    python arbitrage_client.py <pool_update.json> [detectors.json]
"""

import socket
import json
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Optional
import sys
import time


class DetectorConnection:
    """Persistent TCP connection to a single C++ detector process."""

    def __init__(self, port: int, seed: int, k: int, host: str = "localhost"):
        self.port = port
        self.seed = seed
        self.k = k
        self.host = host
        self.sock: Optional[socket.socket] = None
        self.lock = threading.Lock()
        self._recv_buffer = ""

    def connect(self):
        """Establish TCP connection to the detector."""
        if self.sock is not None:
            return
        self.sock = socket.create_connection((self.host, self.port), timeout=10)
        self._recv_buffer = ""

    def close(self):
        """Close the connection."""
        if self.sock:
            try:
                self.sock.close()
            except Exception:
                pass
            self.sock = None

    def send_update(self, pool_json_line: str) -> Optional[dict]:
        """
        Send a pool update and receive the response.

        Args:
            pool_json_line: single-line JSON string of pool state

        Returns:
            dict with keys: weight, profitable, update_us, route (if profitable)
            None if communication failed
        """
        with self.lock:
            try:
                if self.sock is None:
                    self.connect()

                # Send update (must end with newline)
                msg = pool_json_line.strip() + "\n"
                self.sock.sendall(msg.encode())

                # Receive response (read until newline)
                while "\n" not in self._recv_buffer:
                    data = self.sock.recv(65536)
                    if not data:
                        self.close()
                        return None
                    self._recv_buffer += data.decode()

                line, self._recv_buffer = self._recv_buffer.split("\n", 1)
                return json.loads(line)

            except Exception as e:
                print(f"  Error on port {self.port}: {e}")
                self.close()
                return None

    def __repr__(self):
        return f"Detector(port={self.port}, seed={self.seed}, k={self.k})"


class ArbitrageClient:
    """
    Manages connections to multiple detector processes.
    Sends pool updates in parallel and returns the best result.
    """

    def __init__(self, detectors: list[DetectorConnection], max_workers: int = None):
        self.detectors = detectors
        self.max_workers = max_workers or len(detectors)
        self.executor = ThreadPoolExecutor(max_workers=self.max_workers)

    @classmethod
    def from_detectors_file(cls, path: str = "detectors.json") -> "ArbitrageClient":
        """Create client from the detectors.json file written by launch_detectors.py."""
        with open(path) as f:
            infos = json.load(f)
        detectors = [
            DetectorConnection(port=info["port"], seed=info["seed"], k=info["k"])
            for info in infos
        ]
        return cls(detectors)

    @classmethod
    def from_ports(cls, ports: list[int]) -> "ArbitrageClient":
        """Create client from a simple list of ports."""
        detectors = [DetectorConnection(port=p, seed=0, k=0) for p in ports]
        return cls(detectors)

    def connect_all(self):
        """Connect to all detectors."""
        for det in self.detectors:
            try:
                det.connect()
                print(f"  Connected to {det}")
            except Exception as e:
                print(f"  Failed to connect to {det}: {e}")

    def close_all(self):
        """Close all connections."""
        for det in self.detectors:
            det.close()
        self.executor.shutdown(wait=False)

    def send_update(self, pool_json: str) -> dict:
        """
        Send a pool update to ALL detectors in parallel.
        Returns the best (most profitable) result.

        Args:
            pool_json: JSON string of pool state update

        Returns:
            dict with keys:
                - best_result: the best response (most negative weight)
                - best_detector: which detector found it
                - all_results: list of (detector, result) pairs
                - total_time_ms: wall-clock time for the parallel update
        """
        start_time = time.time()

        # Submit to all detectors in parallel
        futures = {
            self.executor.submit(det.send_update, pool_json): det
            for det in self.detectors
        }

        all_results = []
        best_result = None
        best_detector = None
        best_weight = float("inf")

        for future in as_completed(futures):
            det = futures[future]
            result = future.result()
            if result is None:
                continue
            all_results.append((det, result))

            weight = result.get("weight", float("inf"))
            if weight < best_weight:
                best_weight = weight
                best_result = result
                best_detector = det

        elapsed_ms = (time.time() - start_time) * 1000

        return {
            "best_result": best_result,
            "best_detector": best_detector,
            "all_results": all_results,
            "total_time_ms": elapsed_ms,
        }

    def send_updates_batch(self, pool_jsons: list[str]) -> list[dict]:
        """
        Send multiple pool updates sequentially, each broadcast to all detectors.

        Args:
            pool_jsons: list of JSON strings

        Returns:
            list of results from send_update()
        """
        results = []
        for i, pool_json in enumerate(pool_jsons):
            result = self.send_update(pool_json)
            results.append(result)

            best = result["best_result"]
            det = result["best_detector"]
            if best and best.get("profitable"):
                print(f"  Update {i+1}: ARBITRAGE! weight={best['weight']:.6f} "
                      f"profit={best['route'].get('profitPct', 0):.4f}% "
                      f"via {det} [{result['total_time_ms']:.1f}ms]")
            elif best:
                print(f"  Update {i+1}: weight={best['weight']:.6f} "
                      f"via {det} [{result['total_time_ms']:.1f}ms]")
            else:
                print(f"  Update {i+1}: no response [{result['total_time_ms']:.1f}ms]")

        return results


def main():
    """Standalone test: send a pool update file to all detectors."""
    if len(sys.argv) < 2:
        print("Usage: python arbitrage_client.py <pool_update.json> [detectors.json]")
        sys.exit(1)

    update_file = sys.argv[1]
    detectors_file = sys.argv[2] if len(sys.argv) > 2 else "detectors.json"

    # Read pool update
    with open(update_file) as f:
        pool_json = f.read().strip()

    # Create client
    print(f"Loading detectors from {detectors_file}...")
    client = ArbitrageClient.from_detectors_file(detectors_file)
    print(f"Connecting to {len(client.detectors)} detectors...")
    client.connect_all()

    # Send update
    print(f"\nSending pool update from {update_file}...")
    result = client.send_update(pool_json)

    # Print results
    print(f"\n{'='*60}")
    print(f"Total time: {result['total_time_ms']:.1f}ms")
    print(f"Responses: {len(result['all_results'])}/{len(client.detectors)}")

    if result["best_result"]:
        best = result["best_result"]
        det = result["best_detector"]
        print(f"\nBest detector: {det}")
        print(f"  Weight: {best['weight']:.6f}")
        print(f"  Profitable: {best.get('profitable', False)}")
        if best.get("profitable") and "route" in best:
            route = best["route"]
            print(f"  From: {route.get('fromAmount')} {route.get('from', '')[:10]}...")
            print(f"  To:   {route.get('toAmount')} {route.get('to', '')[:10]}...")
            print(f"  Profit: {route.get('profitPct', 0):.4f}%")
            print(f"\n  Route JSON:")
            print(json.dumps(route, indent=2))

    print(f"\nAll results:")
    for det, res in result["all_results"]:
        profitable = res.get("profitable", False)
        marker = " <<<" if profitable else ""
        print(f"  {det}: weight={res['weight']:.6f} "
              f"update={res.get('update_us', 0)}us{marker}")

    client.close_all()


if __name__ == "__main__":
    main()
