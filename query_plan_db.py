# query_plan_db.py
import common_db
import storage_db

def extract_sfw_data():
    node = common_db.global_syn_tree
    sel_list = []
    from_list = []
    where_list = []
    
    if node.value == 'SFW':
        # 处理SELECT子句
        sel_node = node.children[0]
        if sel_node.value == 'SelList':
            if sel_node.children[0].value == 'STAR':
                sel_list.append('*')
            else:
                for child in sel_node.children:
                    if child.value != ',':
                        sel_list.append(child.value)
        
        # 处理FROM子句
        from_node = node.children[1]
        if from_node.value == 'FromList':
            for child in from_node.children:
                if child.value != ',':
                    from_list.append(child.value)
        
        # 处理WHERE子句
        where_node = node.children[2]
        if where_node.value == 'Cond':
            where_list.append((where_node.children[0].value, where_node.children[1].value))
    
    return sel_list, from_list, where_list

def construct_logical_tree():
    sel_list, from_list, where_list = extract_sfw_data()
    
    # 构建逻辑计划树
    root = common_db.Node('Project', [])
    root.var = sel_list  # 存储投影字段列表
    
    if where_list:
        filter_node = common_db.Node('Filter', [])
        filter_node.var = where_list  # 存储过滤条件
        root.children.append(filter_node)
        scan_node = common_db.Node('Scan', [])
        scan_node.var = from_list[0]  # 存储表名（只取第一个表名）
        filter_node.children.append(scan_node)
    else:
        scan_node = common_db.Node('Scan', [])
        scan_node.var = from_list[0]  # 存储表名（只取第一个表名）
        root.children.append(scan_node)
    
    common_db.global_logical_tree = root
    return root

def execute_logical_tree():
    if not common_db.global_logical_tree:
        return None
    
    # 获取表名
    scan_node = common_db.global_logical_tree.children[0]
    if scan_node.value == 'Filter':
        scan_node = scan_node.children[0]
    table_name = scan_node.var
    
    # 确保表名是字符串
    if isinstance(table_name, bytes):
        table_name = table_name.decode('utf-8')
    elif isinstance(table_name, list):
        table_name = table_name[0]
    
    storage = storage_db.Storage(table_name)
    records = storage.getRecord()
    field_list = storage.getFieldList()
    
    # 提取字段名
    field_names = [field[0].decode('utf-8').strip() if isinstance(field[0], bytes) else field[0].strip() for field in field_list]
    
    # 处理WHERE条件
    where_conditions = []
    if common_db.global_logical_tree.children[0].value == 'Filter':
        where_conditions = common_db.global_logical_tree.children[0].var
    
    # 应用WHERE过滤
    filtered_records = []
    for record in records:
        match = True
        for field, value in where_conditions:
            if field in field_names:
                idx = field_names.index(field)
                record_value = record[idx]
                if isinstance(record_value, bytes):
                    record_value = record_value.decode('utf-8')
                if str(record_value).strip() != value.strip("'\""):
                    match = False
                    break
        if match:
            filtered_records.append(record)
    
    # 处理SELECT投影
    result = []
    for record in (filtered_records if where_conditions else records):
        if common_db.global_logical_tree.var == ['*']:
            result.append(record)
        else:
            projected = []
            for field in common_db.global_logical_tree.var:
                if field in field_names:
                    idx = field_names.index(field)
                    value = record[idx]
                    if isinstance(value, bytes):
                        value = value.decode('utf-8')
                    projected.append(value)
            result.append(tuple(projected))
    
    return result