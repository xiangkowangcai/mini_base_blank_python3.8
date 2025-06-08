# main.py
import lex_db
import parser_db
import query_plan_db
import common_db
import storage_db

def test_sfw_query(sql):
    print(f"\n测试SQL: {sql}")
    
    # 词法分析
    lexer = lex_db.Lexer(sql)
    tokens = lexer.tokenize()
    print("词法分析结果:", tokens)
    
    # 语法分析
    parser = parser_db.Parser(tokens)
    common_db.global_syn_tree = parser.parse()
    print("\n语法树:")
    common_db.global_syn_tree.print_tree()
    
    # 构建逻辑计划树
    logical_tree = query_plan_db.construct_logical_tree()
    print("\n逻辑计划树:")
    common_db.show(logical_tree)
    
    # 执行查询
    result = query_plan_db.execute_logical_tree()
    print("\n查询结果:")
    for row in result:
        print(row)

def main():
    # 测试用例
    test_queries = [
        "select * from students;",
        "select name from students;",
        "select name from students where age=19;"
    ]
    
    for query in test_queries:
        test_sfw_query(query)

if __name__ == "__main__":
    main() 