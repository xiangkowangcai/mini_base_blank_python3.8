# -----------------------------------------------------------------------
# storage_db.py
# Author: Jingyu Han  hjymail@163.com
# -----------------------------------------------------------------------
# the module is to store tables in files
# Each table is stored in a separate file with the suffix ".dat".
# For example, the table named moviestar is stored in file moviestar.dat 
# -----------------------------------------------------------------------

# struct of file is as follows, each block is 4096
# ---------------------------------------------------
# block_0|block_1|...|block_n
# ----------------------------------------------------------------
from common_db import BLOCK_SIZE

# structure of block_0, which stores the meta information and field information
# ---------------------------------------------------------------------------------
# block_id                                # 0
# number_of_dat_blocks                    # at first it is 0 because there is no data in the table
# number_of_fields or number_of_records   # the total number of fields for the table
# -----------------------------------------------------------------------------------------


# the data type is as follows
# ----------------------------------------------------------
# 0->str,1->varstr,2->int,3->bool
# ---------------------------------------------------------------


# structure of data block, whose block id begins with 1
# ----------------------------------------
# block_id       
# number of records
# record_0_offset         # it is a pointer to the data of record
# record_1_offset
# ...
# record_n_offset
# ....
# free space
# ...
# record_n
# ...
# record_1
# record_0
# -------------------------------------------

# structre of one record
# -----------------------------
# pointer                     #offset of table schema in block id 0
# length of record            # including record head and record content
# time stamp of last update  # for example,1999-08-22
# field_0_value
# field_1_value
# ...
# field_n_value
# -------------------------


import struct
import os
import ctypes


# --------------------------------------------
# the class can store table data into files
# functions include insert, delete and update
# --------------------------------------------

class Storage(object):

    # ------------------------------
    # constructor of the class
    # input:
    #       tablename
    # -------------------------------------
    def __init__(self, tablename):
        # print "__init__ of ",Storage.__name__,"begins to execute"
        self.tablename = tablename.strip()
        self.record_list = []
        self.record_Position = []
        self.open = False

        if isinstance(self.tablename, bytes):
            tablename_str = self.tablename.decode('utf-8')
            dat_suffix = b'.dat'
        else:
            tablename_str = self.tablename
            dat_suffix = '.dat'

        filepath = self.tablename + dat_suffix

        if not os.path.exists(filepath):
            print('table file ' + tablename_str + '.dat does not exists')
            self.f_handle = open(filepath, 'wb+')
            self.f_handle.close()
            print(tablename_str + '.dat has been created')

        self.f_handle = open(filepath, 'rb+')
        print('table file ' + tablename_str + '.dat has been opened')
        self.open = True

        self.dir_buf = ctypes.create_string_buffer(BLOCK_SIZE)
        self.f_handle.seek(0)
        self.dir_buf = self.f_handle.read(BLOCK_SIZE)

        self.dir_buf.strip()
        my_len = len(self.dir_buf)
        self.field_name_list = []
        beginIndex = 0

        if my_len == 0:  # there is no data in the block 0, we should write meta data into the block 0
            if isinstance(self.tablename, bytes):
                self.num_of_fields = input(
                    "please input the number of fields in table " + tablename_str + ": ")
            else:
                self.num_of_fields = input(
                    "please input the number of fields in table " + tablename_str + ": ")
            if int(self.num_of_fields) > 0:

                self.dir_buf = ctypes.create_string_buffer(BLOCK_SIZE)
                self.block_id = 0
                self.data_block_num = 0
                struct.pack_into('!iii', self.dir_buf, beginIndex, 0, 0,
                                 int(self.num_of_fields))  # block_id,number_of_data_blocks,number_of_fields

                beginIndex = beginIndex + struct.calcsize('!iii')

                # the following is to write the field name, field type and field length into the buffer in turn
                for i in range(int(self.num_of_fields)):
                    field_name = input("please input the name of field " + str(i) + " : ").strip()

                    # 检查字段名长度是否超过10个字节
                    if len(field_name) > 10:
                        print("字段名长度不能超过10个字节，请重新输入。")
                        field_name = input("please input the name of field " + str(i) + " : ").strip()

                    # 确保字段名长度为10个字节
                    if len(field_name) < 10:
                        field_name = field_name.ljust(10)  # 左对齐并填充空格到10个字节

                    while True:
                        field_type = input(
                            "please input the type of field(0-> str; 1-> varstr; 2-> int; 3-> boolean) " + str(
                                i) + " : ").strip()
                        if int(field_type) in [0, 1, 2, 3]:
                            break
                        else:
                            print("无效的字段类型，请重新输入。")

                    # 输入字段长度并验证其有效性
                    while True:
                        field_length_input = input("please input the length of field " + str(i) + " : ").strip()
                        try:
                            field_length = int(field_length_input)
                            if field_length > 0:
                                break
                            else:
                                print("字段长度必须是正整数，请重新输入。")
                        except ValueError:
                            print("请输入一个有效的整数作为字段长度。")

                    # 创建临时元组并添加到字段列表中
                    temp_tuple = (field_name, int(field_type), field_length)
                    self.field_name_list.append(temp_tuple)

                    # 将字段名编码为UTF-8
                    if isinstance(field_name, str):
                        field_name = field_name.encode('utf-8')

                    # 将字段名、字段类型和字段长度打包到缓冲区中
                    struct.pack_into('!10sii', self.dir_buf, beginIndex, field_name, int(field_type), field_length)
                    beginIndex = beginIndex + struct.calcsize('!10sii')

                # 将缓冲区的内容写入文件头
                self.f_handle.seek(0)
                self.f_handle.write(self.dir_buf)
                self.f_handle.flush()

        else:  # there is something in the file

            self.block_id, self.data_block_num, self.num_of_fields = struct.unpack_from('!iii', self.dir_buf, 0)

            print('number of fields is ', self.num_of_fields)
            print('data_block_num', self.data_block_num)
            beginIndex = struct.calcsize('!iii')

            # the followings is to read field name, field type and field length into main memory structures
            for i in range(self.num_of_fields):
                field_name, field_type, field_length = struct.unpack_from('!10sii', self.dir_buf,
                                                                          beginIndex + i * struct.calcsize(
                                                                              '!10sii'))  # i means no memory alignment

                temp_tuple = (field_name, field_type, field_length)
                self.field_name_list.append(temp_tuple)
                print("the " + str(i) + "th field information (field name,field type,field length) is ", temp_tuple)
        # print self.field_name_list
        record_head_len = struct.calcsize('!ii10s')
        record_content_len = sum(map(lambda x: x[2], self.field_name_list))
        # print record_content_len

        Flag = 1
        while Flag <= self.data_block_num:
            self.f_handle.seek(BLOCK_SIZE * Flag)
            self.active_data_buf = self.f_handle.read(BLOCK_SIZE)
            self.block_id, self.Number_of_Records = struct.unpack_from('!ii', self.active_data_buf, 0)
            print('Block_ID=%s,   Contains %s data' % (self.block_id, self.Number_of_Records))
            # There exists record
            if self.Number_of_Records > 0:
                for i in range(self.Number_of_Records):
                    self.record_Position.append((Flag, i))
                    offset = \
                        struct.unpack_from('!i', self.active_data_buf,
                                           struct.calcsize('!ii') + i * struct.calcsize('!i'))[0]
                    record = struct.unpack_from('!' + str(record_content_len) + 's', self.active_data_buf,
                                                offset + record_head_len)[0]
                    tmp = 0
                    tmpList = []
                    for field in self.field_name_list:
                        t = record[tmp:tmp + field[2]].strip()
                        tmp = tmp + field[2]
                        if field[1] == 2:
                            t = int(t)
                        if field[1] == 3:
                            t = bool(t)
                        tmpList.append(t)
                    self.record_list.append(tuple(tmpList))
            Flag += 1

    # ------------------------------
    # return the record list of the table
    # input:
    #       
    # -------------------------------------
    def getRecord(self):
        return self.record_list

    # --------------------------------
    # to insert a record into table
    # param insert_record: list
    # return: True or False
    # -------------------------------
    def insert_record(self, insert_record):

        # example: ['xuyidan','23','123456']

        # step 1 : to check the insert_record is True or False

        tmpRecord = []
        for idx in range(len(self.field_name_list)):
            insert_record[idx] = insert_record[idx].strip()
            if self.field_name_list[idx][1] == 0 or self.field_name_list[idx][1] == 1:
                if len(insert_record[idx]) > self.field_name_list[idx][2]:
                    return False
                tmpRecord.append(insert_record[idx])
            if self.field_name_list[idx][1] == 2:
                try:
                    tmpRecord.append(int(insert_record[idx]))
                except:
                    return False
            if self.field_name_list[idx][1] == 3:
                try:
                    tmpRecord.append(bool(insert_record[idx]))
                except:
                    return False
            insert_record[idx] = ' ' * (self.field_name_list[idx][2] - len(insert_record[idx])) + insert_record[idx]

        # step2: Add tmpRecord to record_list ; change insert_record into inputstr
        inputstr = ''.join(insert_record)

        self.record_list.append(tuple(tmpRecord))

        # Step3: To calculate MaxNum in each Data Blocks
        record_content_len = len(inputstr)
        record_head_len = struct.calcsize('!ii10s')
        record_len = record_head_len + record_content_len
        MAX_RECORD_NUM = (BLOCK_SIZE - struct.calcsize('!i') - struct.calcsize('!ii')) // (
                record_len + struct.calcsize('!i'))

        # Step4: To calculate new record Position
        if not len(self.record_Position):
            self.data_block_num += 1
            self.record_Position.append((1, 0))
        else:
            last_Position = self.record_Position[-1]
            if last_Position[1] == MAX_RECORD_NUM - 1:
                self.record_Position.append((last_Position[0] + 1, 0))
                self.data_block_num += 1
            else:
                self.record_Position.append((last_Position[0], last_Position[1] + 1))

        last_Position = self.record_Position[-1]

        # Step5: Write new record into file xxx.dat
        # update data_block_num
        self.f_handle.seek(0)
        self.buf = ctypes.create_string_buffer(struct.calcsize('!iii'))
        struct.pack_into('!iii', self.buf, 0, 0, self.data_block_num, int(self.num_of_fields))
        self.f_handle.write(self.buf)
        self.f_handle.flush()

        # update data block head
        self.f_handle.seek(BLOCK_SIZE * last_Position[0])
        self.buf = ctypes.create_string_buffer(struct.calcsize('!ii'))
        struct.pack_into('!ii', self.buf, 0, last_Position[0], last_Position[1] + 1)
        self.f_handle.write(self.buf)
        self.f_handle.flush()

        # update data offset
        offset = struct.calcsize('!ii') + last_Position[1] * struct.calcsize('!i')
        beginIndex = BLOCK_SIZE - (last_Position[1] + 1) * record_len
        self.f_handle.seek(BLOCK_SIZE * last_Position[0] + offset)
        self.buf = ctypes.create_string_buffer(struct.calcsize('!i'))
        struct.pack_into('!i', self.buf, 0, beginIndex)
        self.f_handle.write(self.buf)
        self.f_handle.flush()

        # update data
        record_schema_address = struct.calcsize('!iii')
        update_time = '2016-11-16'  # update time
        self.f_handle.seek(BLOCK_SIZE * last_Position[0] + beginIndex)
        self.buf = ctypes.create_string_buffer(record_len)
        struct.pack_into('!ii10s', self.buf, 0, record_schema_address, record_content_len, update_time.encode('utf-8'))
        struct.pack_into('!' + str(record_content_len) + 's', self.buf, record_head_len, inputstr.encode('utf-8'))
        self.f_handle.write(self.buf.raw)
        self.f_handle.flush()

        return True

    # ------------------------------
    # show the data structure and its data
    # input:
    #       t
    # -------------------------------------

    def show_table_data(self):
        print('|    '.join(map(lambda x: x[0].decode('utf-8').strip(), self.field_name_list)))  # show the structure

        # the following is to show the data of the table
        for record in self.record_list:
            print(record)

    # --------------------------------
    # to delete  the data file
    # input
    #       table name
    # output
    #       True or False
    # -----------------------------------
    def delete_table_data(self, tableName):
        if self.open == True:
            self.f_handle.close()
            self.open = False

        tableName = tableName.strip()
        if isinstance(tableName, bytes):
            dat_suffix = b'.dat'
        else:
            dat_suffix = '.dat'
        
        filepath = tableName + dat_suffix
        if os.path.exists(filepath):
            os.remove(filepath)

        return True

    # ------------------------------
    # get the list of field information, each element of which is (field name, field type, field length)
    # input:
    #       
    # -------------------------------------

    def getFieldList(self):
        return self.field_name_list

    def delete_row_by_keyword(self, field_name, keyword):
        """
        根据字段名和关键字删除行
        :param field_name: 字段名
        :param keyword: 关键字
        :return: 是否删除成功
        """
        # 获取字段索引
        field_names = [field[0].decode('utf-8').strip() if isinstance(field[0], bytes) else field[0].strip() 
                      for field in self.field_name_list]
        if field_name not in field_names:
            print(f"字段 {field_name} 不存在")
            return False
        
        field_idx = field_names.index(field_name)
        
        # 查找匹配的记录
        records_to_delete = []
        for i, record in enumerate(self.record_list):
            record_value = record[field_idx]
            if isinstance(record_value, bytes):
                record_value = record_value.decode('utf-8')
            if str(record_value).strip() == keyword.strip():
                records_to_delete.append(i)
        
        if not records_to_delete:
            print(f"未找到匹配的记录")
            return False
        
        # 删除记录
        for i in sorted(records_to_delete, reverse=True):
            del self.record_list[i]
        
        # 更新文件
        self.f_handle.seek(0)
        self.dir_buf = ctypes.create_string_buffer(BLOCK_SIZE)
        struct.pack_into('!iii', self.dir_buf, 0, 0, 1, self.num_of_fields)
        
        # 写入字段信息
        beginIndex = struct.calcsize('!iii')
        for field in self.field_name_list:
            struct.pack_into('!10sii', self.dir_buf, beginIndex, field[0], field[1], field[2])
            beginIndex += struct.calcsize('!10sii')
        
        # 写入数据块
        data_buf = ctypes.create_string_buffer(BLOCK_SIZE)
        struct.pack_into('!ii', data_buf, 0, 1, len(self.record_list))
        
        # 计算记录偏移量
        record_head_len = struct.calcsize('!ii10s')
        record_content_len = sum(map(lambda x: x[2], self.field_name_list))
        offset = struct.calcsize('!ii') + len(self.record_list) * struct.calcsize('!i')
        
        # 写入记录偏移量
        for i in range(len(self.record_list)):
            struct.pack_into('!i', data_buf, struct.calcsize('!ii') + i * struct.calcsize('!i'), offset)
            offset += record_head_len + record_content_len
        
        # 写入记录内容
        for i, record in enumerate(self.record_list):
            record_data = b''
            for value in record:
                if isinstance(value, str):
                    value = value.encode('utf-8')
                elif isinstance(value, int):
                    value = str(value).encode('utf-8')
                elif isinstance(value, bool):
                    value = str(value).encode('utf-8')
                record_data += value.ljust(10)[:10]  # 确保每个字段都是10字节
            
            struct.pack_into('!ii10s' + str(record_content_len) + 's', 
                           data_buf, 
                           struct.calcsize('!ii') + len(self.record_list) * struct.calcsize('!i') + i * (record_head_len + record_content_len),
                           0,  # block_id
                           record_head_len + record_content_len,  # record length
                           b'0',  # timestamp
                           record_data)
        
        # 写入文件
        self.f_handle.write(self.dir_buf)
        self.f_handle.write(data_buf)
        self.f_handle.flush()
        
        print(f"成功删除 {len(records_to_delete)} 条记录")
        return True

    # ----------------------------------------
    # destructor
    # ------------------------------------------------
    def __del__(self):
        if hasattr(self, 'open') and self.open:
            self.close()

    def close(self):
        if self.open == True:
            self.f_handle.seek(0)
            self.buf = ctypes.create_string_buffer(struct.calcsize('!iii'))
            struct.pack_into('!iii', self.buf, 0, 0, self.data_block_num, int(self.num_of_fields))
            self.f_handle.write(self.buf)
            self.f_handle.flush()
            self.f_handle.close()
            if isinstance(self.tablename, bytes):
                tablename_str = self.tablename.decode('utf-8')
            else:
                tablename_str = self.tablename
            print('table file ' + tablename_str + '.dat has been closed')
            self.open = False
