from main import BPlusTree
from node import Node

n = Node(page_offset=1)
n.keys = range(1,10001)
n.values = [str(i) for i in range(1,10001)]
tree = BPlusTree.create("test.db", 4096, 1000)
for i in range(10):
    p, l, r = tree.__split_node(n)
    print(f"{p.page_offset}  {l.page_offset}  {r.page_offset}")