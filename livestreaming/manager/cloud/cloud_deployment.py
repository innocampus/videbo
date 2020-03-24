import os


# TODO: Error handling
def init_content_node(ip):
    os.system(f"export ANSIBLE_HOST_KEY_CHECKING=False; ansible-playbook -i root@{ip}, ../../ansible/init_content_node.yml")
