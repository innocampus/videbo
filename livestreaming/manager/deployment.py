from cloud import HetznerAPI
from cloud import init_content_node
import time

hetzner = HetznerAPI('xxx')

print("current state:")
for n in hetzner.get_all_nodes():
	print(n)

node = hetzner.create_node("testnode")
print("sleep...")
time.sleep(30)

print("current state:")
for n in hetzner.get_all_nodes():
	print(n)

init_content_node(node.ip)
