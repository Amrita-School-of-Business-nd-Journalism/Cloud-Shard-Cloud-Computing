import csv
import io
import json
import urllib.request
from pathlib import Path

import grpc

import shard_pb2
import shard_pb2_grpc


FLASK_SERVER = "http://localhost:5000"
REQUEST_TIMEOUT = 120


def get_node_endpoints() -> list[str]:
    url = f"{FLASK_SERVER}/api/nodes"

    with urllib.request.urlopen(url, timeout=15) as response:
        data = json.loads(response.read().decode("utf-8"))

    return [
        node["endpoint"]
        for node in data["nodes"]
        if node.get("endpoint")
    ]


def get_shards_from_node(endpoint: str, dataset_id: str) -> list[dict]:
    with grpc.insecure_channel(
        endpoint,
        options=[
            ("grpc.max_receive_message_length", 64 * 1024 * 1024),
        ],
    ) as channel:
        stub = shard_pb2_grpc.ShardStub(channel)
        response = stub.listShards(
            shard_pb2.ListShardsRequest(dataset_id_=dataset_id),
            timeout=REQUEST_TIMEOUT,
        )

        return [
            {
                "shard_id": shard.shard_id_,
                "size": shard.size_,
                "data": shard.data_,
                "sequence_number": getattr(
                    shard, "dataset_shard_number", None
                ),
            }
            for shard in response.shards
        ]


def load_dataset_csv(dataset_id: str) -> io.StringIO:
    endpoints = get_node_endpoints()

    if not endpoints:
        raise RuntimeError("No online storage nodes found.")

    all_shards = []

    for endpoint in endpoints:
        try:
            all_shards.extend(
                get_shards_from_node(endpoint, dataset_id)
            )
        except grpc.RpcError as exc:
            raise RuntimeError(
                f"Failed to retrieve shards from {endpoint}: "
                f"{exc.code().name}: {exc.details()}"
            ) from exc

    if not all_shards:
        raise RuntimeError(
            f"No shards found for dataset {dataset_id!r}."
        )

    if any(s["sequence_number"] is not None for s in all_shards):
        if any(s["sequence_number"] is None for s in all_shards):
            raise RuntimeError(
                "Some shards are missing global sequence numbers."
            )

        all_shards.sort(key=lambda s: s["sequence_number"])
    else:
        # Local IDs cannot guarantee original ordering across nodes.
        all_shards.sort(key=lambda s: int(s["shard_id"]))

    csv_data = b"".join(shard["data"] for shard in all_shards)
    return io.StringIO(csv_data.decode("utf-8-sig"))


class CloudShard:
    def __init__(self, dataset_id: str):
        self.dataset_id = dataset_id
        self._load()

    def _load(self):
        self._stream = load_dataset_csv(self.dataset_id)
        self._reader = csv.reader(self._stream)
        self._rows = list(self._reader)
        self._headers = self._rows[0] if self._rows else []
        self._data = self._rows[1:] if self._rows else []

    def __len__(self):
        return len(self._data)

    def __iter__(self):
        return iter(self._data)

    def __getitem__(self, index):
        if isinstance(index, str):
            if index not in self._headers:
                raise KeyError(f"Column {index!r} does not exist")

            column_index = self._headers.index(index)
            return [row[column_index] for row in self._data]

        if isinstance(index, tuple):
            row_index, column = index
            row = self._data[row_index]

            if isinstance(column, str):
                if column not in self._headers:
                    raise KeyError(f"Column {column!r} does not exist")
                column = self._headers.index(column)

            return row[column]

        return self._data[index]

    @property
    def columns(self):
        return self._headers

    def to_list(self):
        return self._data

    def to_dicts(self):
        return [
            dict(zip(self._headers, row))
            for row in self._data
        ]

    def refresh(self):
        self._load()

    def save_csv(self, output_path: str | Path = "saved/new_name.csv"):
        """Save the reconstructed dataset as a CSV file."""
        path = Path(output_path)
        path.parent.mkdir(parents=True, exist_ok=True)

        with path.open("w", newline="", encoding="utf-8") as output_file:
            writer = csv.writer(output_file)
            writer.writerow(self._headers)
            writer.writerows(self._data)

        return path


if __name__ == "__main__":
    dataset_id = input("Dataset ID: ").strip()
    dataset = CloudShard(dataset_id)

    print("Columns:", dataset.columns)
    print("Rows:", len(dataset))

    if dataset:
        print(dataset[0])

    if dataset.columns:
        print(dataset[dataset.columns[0]])
        if len(dataset):
            print(dataset[0, dataset.columns[0]])

    output_path = dataset.save_csv("saved/new_name.csv")
    print(f"Dataset saved to: {output_path.resolve()}")