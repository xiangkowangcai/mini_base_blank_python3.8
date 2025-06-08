# node_db.py
# 语法树节点定义模块

class Node:
    def __init__(self, value, children=None):
        self.value = value
        self.children = children if children is not None else []
        self.var = None  # 用于存储额外的变量信息
        
    def add_child(self, child):
        self.children.append(child)
        
    def __str__(self):
        return f"{self.value}"
        
    def print_tree(self, level=0):
        print("  " * level + str(self))
        for child in self.children:
            child.print_tree(level + 1) 