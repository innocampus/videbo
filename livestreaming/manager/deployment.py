from cloud import HetznerAPI
from cloud import init_content_node

hetzner = HetznerAPI('xxx')

print("current state:")
for n in hetzner.get_all_nodes():
	print(n)


