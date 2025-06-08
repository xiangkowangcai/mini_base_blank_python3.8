# log_manager.py  ——新增 作者：XX
import os
import struct

LOG_ENTRY_HEADER = struct.Struct('>I I I')  
# 格式：txn_id(uint32) | block_id(uint32) | data_len(uint32)

class LogManager:
    """
    WAL 日志管理器。
    before_f: 前像日志文件句柄
    after_f:  后像日志文件句柄
    active:  活动事务 ID 集合
    committed: 已提交事务 ID 集合
    """
    def __init__(self, dirname='.'):
        os.makedirs(dirname, exist_ok=True)
        self.before_path = os.path.join(dirname, 'log_before.dat')
        self.after_path  = os.path.join(dirname, 'log_after.dat')
        self.before_f = open(self.before_path, 'ab+')
        self.after_f  = open(self.after_path, 'ab+')
        self.active    = set()
        self.committed = set()

    def begin(self, txn_id: int):
        """事务开始，注册活动事务"""
        self.active.add(txn_id)

    def write_before(self, txn_id: int, block_id: int, data: bytes):
        """写前像：在变更前持久化原始数据"""
        entry = LOG_ENTRY_HEADER.pack(txn_id, block_id, len(data)) + data
        self.before_f.write(entry)
        self.before_f.flush()  # 先记

    def write_after(self, txn_id: int, block_id: int, data: bytes):
        """写后像：在逻辑变更后持久化新数据"""
        entry = LOG_ENTRY_HEADER.pack(txn_id, block_id, len(data)) + data
        self.after_f.write(entry)
        self.after_f.flush()

    def commit(self, txn_id: int):
        """
        提交事务：保证后像已写入（WAL），
        然后在内存中标记已提交。
        """
        if txn_id not in self.active:
            raise RuntimeError(f"txn {txn_id} not active")
        self.committed.add(txn_id)
        self.active.remove(txn_id)

    def recover(self, storage):
        """
        系统启动或崩溃恢复时调用：
        1) 重做所有后像（redo）对已提交事务
        2) （可选）撤销未提交事务的前像（undo）
        """
        # 先读 committed set（可从持久化文件加载）
        # 1. Redo
        with open(self.after_path, 'rb') as f:
            while True:
                header = f.read(LOG_ENTRY_HEADER.size)
                if not header: break
                txn_id, block_id, length = LOG_ENTRY_HEADER.unpack(header)
                data = f.read(length)
                if txn_id in self.committed:
                    storage._apply_redo(block_id, data)
        # 2. Undo（略，可按需实现）
