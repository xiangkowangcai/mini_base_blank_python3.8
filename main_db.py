# -*- coding: utf-8 -*-
# -----------------------
# main_db.py
# author: Jingyu Han   hjymail@163.com
# modified by: Ning Wang, Yidan Xu
# -----------------------------------
# This is the main loop of the program
# ---------------------------------------

import struct
import sys
import ctypes
import os

import head_db  # the main memory structure of table schema
import schema_db  # the module to process table schema
import storage_db  # the module to process the storage of instance
import query_plan_db  # for SQL clause of which data is stored in binary format
import lex_db  # for lex, where data is stored in binary format
import parser_db  # for parser
import common_db  # the global variables, functions, constants in the program

PROMPT_STR = 'Input your choice  \n1:add a new table structure and data \n2:delete a table structure and data\
\n3:view a table structure and data \n4:delete all tables and data \n5:select from where clause\
\n6:delete a row according to field keyword \n7:update a row according to field keyword \n. to quit):\n'


# --------------------------
# the main loop, which needs further implementation
# ---------------------------

def main():
    # main loops for the whole program
    print('main function begins to execute')

    # The instance data of table is stored in binary format, which corresponds to chapter 2-8 of textbook

    schemaObj = schema_db.Schema()  # to create a schema object, which contains the schema of all tables
    dataObj = None
    choice = input(PROMPT_STR)

    while True:

        if choice == '1':  # add a new table and lines of data
            tableName = input('please enter your new table name:')
            if isinstance(tableName, str):
                tableName = tableName.encode('utf-8')
            #  tableName not in all.sch
            insertFieldList = []
            if tableName.strip() not in schemaObj.get_table_name_list():
                # Create a new table
                dataObj = storage_db.Storage(tableName)

                insertFieldList = dataObj.getFieldList()

                schemaObj.appendTable(tableName, insertFieldList)  # add the table structure
            else:
                dataObj = storage_db.Storage(tableName)

                # to the students: The following needs to be further implemented (many lines can be added)
                record = []
                Field_List = dataObj.getFieldList()
                for x in Field_List:
                    s = 'Input field name is: ' + str(x[0].strip()) + '  field type is: ' + str(x[1]) + \
                        ' field maximum length is: ' + str(x[2]) + '\n'
                    record.append(input(s))

                if dataObj.insert_record(record):  # add a row
                    print('OK!')
                else:
                    print('Wrong input!')

                del dataObj

            choice = input(PROMPT_STR)

        elif choice == '2':  # delete a table from schema file and data file

            table_name = input('please input the name of the table to be deleted:')
            if isinstance(table_name, str):
                table_name = table_name.encode('utf-8')
            if schemaObj.find_table(table_name.strip()):
                if schemaObj.delete_table_schema(
                        table_name):  # delete the schema from the schema file
                    dataObj = storage_db.Storage(table_name)  # create an object for the data of table
                    dataObj.delete_table_data(table_name.strip())  # delete table content from the table file
                    del dataObj

                else:
                    print('the deletion from schema file fail')


            else:
                print('there is no table ' + table_name.decode('utf-8') + ' in the schema file')


            choice = input(PROMPT_STR)

        elif choice == '3':  # view the table structure and all the data

            print(schemaObj.headObj.tableNames)
            table_name = input('please input the name of the table to be displayed:')
            if isinstance(table_name, str):
                table_name = table_name.encode('utf-8')
            if table_name.strip():
                if schemaObj.find_table(table_name.strip()):
                    schemaObj.viewTableStructure(table_name)  # to be implemented

                    dataObj = storage_db.Storage(table_name)  # create an object for the data of table
                    dataObj.show_table_data()  # view all the data of the table
                    del dataObj
                else:
                    print('table name is None')

            choice = input(PROMPT_STR)

        elif choice == '4':  # delete all the table structures and their data
            table_name_list = list(schemaObj.get_table_name_list())
            # to be inserted here -> to delete from data files
            for i in range(len(table_name_list)):
                table_name = table_name_list[i]
                table_name.strip()

                if table_name:
                    stObj = storage_db.Storage(table_name)
                    stObj.delete_table_data(table_name.strip())  # delete table data
                    del stObj

            schemaObj.deleteAll()  # delete schema from schema file

            choice = input(PROMPT_STR)

        elif choice == '5':  # process SELECT FROM WHERE clause
            print('#        Your Query is to SQL QUERY                  #')
            sql_str = input('please enter the select from where clause:')
            
            try:
                # 词法分析
                lexer = lex_db.Lexer(sql_str)
                tokens = lexer.tokenize()
                
                # 语法分析
                parser = parser_db.Parser(tokens)
                common_db.global_syn_tree = parser.parse()
                
                # 构建和执行查询计划
                query_plan_db.construct_logical_tree()
                result = query_plan_db.execute_logical_tree()
                
                if result:
                    print("\nQuery result:")
                    for row in result:
                        print(row)
                else:
                    print("\nNo results found.")
            except Exception as e:
                print(f'\nError: {str(e)}')
                
            print('#----------------------------------------------------#')
            choice = input(PROMPT_STR)

        elif choice == '6':  # delete a line of data from the storage file given the keyword
            table_name = input('please input the name of the table to be deleted from:')
            if isinstance(table_name, str):
                table_name = table_name.encode('utf-8')
            field_name, keyword = input('please input the field name and the corresponding keyword (fieldname:keyword):').split(':') # split the input into field name and keyword
            dataObj = storage_db.Storage(table_name)
            if dataObj.delete_row_by_keyword(field_name, keyword):
                print('Row deleted successfully.')
            else:
                print('No matching row found.')
            del dataObj
            choice = input(PROMPT_STR) # get the next input

        elif choice == '7':  # update a line of data given the keyword
            table_name = input('please input the name of the table:')
            if isinstance(table_name, str):
                table_name = table_name.encode('utf-8')
            field_name = input('please input the field name:') # field name
            old_value = input('please input the old value of the field:')  # old field value
            
            new_value = input('please input the new value of the field:') # new field value
            dataObj = storage_db.Storage(table_name) 
            if dataObj.update_row_by_keyword(field_name, old_value, new_value):
                print('Row updated successfully.')
            else:
                print('No matching row found.')
            del dataObj
            choice = input(PROMPT_STR)

        elif choice == '.':
            print('main loop finishies')
            del schemaObj
            break

    print('main loop finish!')


if __name__ == '__main__':
    main()
