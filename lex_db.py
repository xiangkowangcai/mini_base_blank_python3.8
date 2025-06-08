# lex_db.py
# 词法分析器模块

class Lexer:
    def __init__(self, text):
        self.text = text
        self.pos = 0
        self.current_char = self.text[0] if text else None
        self.tokens = []
        
    def error(self):
        raise Exception('词法分析错误')
        
    def advance(self):
        self.pos += 1
        if self.pos > len(self.text) - 1:
            self.current_char = None
        else:
            self.current_char = self.text[self.pos]
            
    def skip_whitespace(self):
        while self.current_char is not None and self.current_char.isspace():
            self.advance()
            
    def id(self):
        result = ''
        while self.current_char is not None and (self.current_char.isalnum() or self.current_char == '_'):
            result += self.current_char
            self.advance()
        # 检查是否是SQL关键字
        result = result.lower()
        if result in ['select', 'from', 'where']:
            return result
        return result
        
    def number(self):
        result = ''
        while self.current_char is not None and self.current_char.isdigit():
            result += self.current_char
            self.advance()
        return result
        
    def get_next_token(self):
        while self.current_char is not None:
            if self.current_char.isspace():
                self.skip_whitespace()
                continue
                
            if self.current_char.isalpha():
                return self.id()
                
            if self.current_char.isdigit():
                return self.number()
                
            if self.current_char == '*':
                self.advance()
                return '*'
                
            if self.current_char == '=':
                self.advance()
                return '='
                
            if self.current_char == ';':
                self.advance()
                return ';'
                
            if self.current_char == ',':
                self.advance()
                return ','
                
            if self.current_char == "'" or self.current_char == '"':
                quote = self.current_char
                self.advance()
                result = ''
                while self.current_char is not None and self.current_char != quote:
                    result += self.current_char
                    self.advance()
                self.advance()  # 跳过结束的引号
                return result
                
            self.error()
            
        return None
        
    def tokenize(self):
        while True:
            token = self.get_next_token()
            if token is None:
                break
            self.tokens.append(token)
        return self.tokens