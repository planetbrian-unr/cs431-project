# basic networkx graph generation prototype concept
# Brian Wu

# built-in
import sqlite3

# pip
import networkx as nx
import matplotlib.pyplot as plt

DB_PATH = "0302/dataset.db"
CHUNK_SIZE = 1_000

# Execute *sql* and yield rows batch‑by‑batch.
def stream_query(sql: str):
    with sqlite3.connect(DB_PATH) as con:
        cur = con.execute(sql)
        while True:
            batch = cur.fetchmany(CHUNK_SIZE)
            if not batch:
                break
            for row in batch:
                yield row

# grab rows from video sql table to form a basic set of nodes for graph
# a node consists of a hashable value and optionally a dictionary of data
def build_nodes(G: nx.Graph) -> None:
    sql = "SELECT * FROM Video"
    for row in stream_query(sql):
        node_id = row[0]
        attrs:dict = {
            "uploader": row[1],
            "age":      row[2],
            "category": row[3],
            "length":   row[4],
            "views":    row[5],
            "rate":     row[6],
            "ratings":  row[7],
            "comments": row[8],
        }
        G.add_node(node_id, **attrs)

# grab N rows from relation sql table to form a basic set of edges for graph
# a edge consists of 2 nodes
def build_edges(G: nx.Graph) -> None:
    sql = "SELECT video_id, related_id FROM Relation"
    edge_gen = ((row[0], row[1]) for row in stream_query(sql))
    # bulk‑add while tqdm tracks progress
    G.add_edges_from(edge_gen)


def main() -> None:
    G = nx.Graph()
    build_nodes(G)
    build_edges(G)

    pos = nx.spring_layout(G, k=0.15, iterations=20)

    nx.draw(
        G,
        pos,
        node_size=20,
        linewidths=0.5,
        edge_color="#888",
        with_labels=False,
    )
    plt.tight_layout()
    plt.savefig("graph.png", dpi=300)
    plt.close()


if __name__ == "__main__":
    main()
