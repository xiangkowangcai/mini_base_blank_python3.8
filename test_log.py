import os, shutil, tempfile, subprocess, time

# 1. 准备临时目录
tmp = tempfile.mkdtemp()
os.chdir(tmp)

# 2. 拷贝项目文件到临时目录（假设项目在 ../proj）
shutil.copytree('D:\zhongzheng\Documents\南邮课程文档\大三下\数据库系统实现\代码\mini_base_blank_python3.8', 'dbms')
os.chdir('dbms')

def run_commands(cmds, timeout=1):
    """启动 main_db.py，依次发送 cmds，然后等待 timeout 秒后杀掉。"""
    p = subprocess.Popen(
        ['python', 'main_db.py'],
        stdin=subprocess.PIPE,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True
    )
    for line in cmds:
        p.stdin.write(line + '\n')
        p.stdin.flush()
    time.sleep(timeout)
    p.kill()
    return p.stdout.read(), p.stderr.read()

# 测试 1：已提交事务能持久化
# 运行并提交
out, err = run_commands(['BEGIN', '1', 'id=1,name=Alice', 'COMMIT', '.'])
assert os.path.exists('logs/log_after.dat')
with open('data.db','rb') as f:
    data = f.read()
assert b'Alice' in data, "提交后 data.db 应包含 Alice"

# 测试 2：未提交事务不应生效
# 运行插入但不提交
out, err = run_commands(['BEGIN', '1', 'id=2,name=Bob'], timeout=0.5)
# 重启，自动恢复
out, err = run_commands(['.'], timeout=0.5)
with open('data.db','rb') as f:
    data2 = f.read()
assert b'Bob' not in data2, "未提交事务不应出现在 data.db"

print("所有 WAL 持久化测试通过！")
