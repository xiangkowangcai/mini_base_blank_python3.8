# parser_db.py
# 语法分析器模块

import node_db
import lex_db
import common_db

class Parser:
    def __init__(self, tokens):
        self.tokens = tokens
        self.pos = 0
        self.current_token = self.tokens[0] if tokens else None
        
    def error(self):
        raise Exception('语法分析错误')
        
    def advance(self):
        self.pos += 1
        if self.pos > len(self.tokens) - 1:
            self.current_token = None
        else:
            self.current_token = self.tokens[self.pos]
            
    def eat(self, token_type):
        if self.current_token == token_type:
            self.advance()
        else:
            self.error()
            
    def parse_select_list(self):
        node = node_db.Node('SelList')
        if self.current_token == '*':
            node.add_child(node_db.Node('STAR'))
            self.advance()
        else:
            while self.current_token and self.current_token != 'from':
                if self.current_token == ',':
                    self.advance()
                node.add_child(node_db.Node(self.current_token))
                self.advance()
        return node
        
    def parse_from_list(self):
        node = node_db.Node('FromList')
        while self.current_token and self.current_token != 'where' and self.current_token != ';':
            if self.current_token == ',':
                self.advance()
            node.add_child(node_db.Node(self.current_token))
            self.advance()
        return node
        
    def parse_where_clause(self):
        if not self.current_token or self.current_token == ';':
            return node_db.Node('NoWhere')
            
        node = node_db.Node('Cond')
        field = self.current_token
        self.advance()
        self.eat('=')
        value = self.current_token
        self.advance()
        node.add_child(node_db.Node(field))
        node.add_child(node_db.Node(value))
        return node
        
    def parse(self):
        if not self.tokens:
            return None
            
        # 解析 SELECT
        if self.current_token.lower() != 'select':
            self.error()
        self.advance()
        
        # 创建根节点
        root = node_db.Node('SFW')
        
        # 解析 SELECT 列表
        root.add_child(self.parse_select_list())
        
        # 解析 FROM
        if self.current_token.lower() != 'from':
            self.error()
        self.advance()
        
        # 解析 FROM 列表
        root.add_child(self.parse_from_list())
        
        # 解析 WHERE
        if self.current_token and self.current_token.lower() == 'where':
            self.advance()
            root.add_child(self.parse_where_clause())
        else:
            root.add_child(node_db.Node('NoWhere'))
            
        # 检查结束符
        if self.current_token and self.current_token != ';':
            self.error()
            
        return root

def set_handle():
    # 为了保持与原有代码的兼容性
    pass