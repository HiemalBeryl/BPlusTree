import random
import secrets
import string
import unittest
from main import BPlusTree


class MyTestCase(unittest.TestCase):
    def test_tree(self):
        tree = BPlusTree.create("test.db", 4096, 100)
        tree.insert(1, "hello, World!")
        tree.insert(2, "GobBye, World!")
        tree.insert(3, "ggez!")
        tree.insert(4, "xpl")
        tree.insert(5, "662666668")
        tree.insert(6, "im a long data 66666666666666666666666666666666666")
        tree.insert(7, "你好")
        tree.insert(8, 1234546789)
        tree.close()

    def test_insert_data(self):
        """插入500w条数据，id为1到5000000的自增int类型整数，values为随机长度字符串"""
        times = 50000
        records = 100
        tree = BPlusTree.create("test.db", 4096, 1000)
        for i in range(times):
            for j in range(records):
                length = secrets.randbelow(10 - 5 + 1) + 5
                characters = string.ascii_letters + string.digits
                secure_random_string = ''.join(secrets.choice(characters) for _ in range(length))
                tree.insert(i * 100 + j + 1, secure_random_string)
        # 平均耗时1200s，数据库文件100MB+
        tree.close()

    def test_read(self):
        tree = BPlusTree.create("test.db", 4096, 1000)
        r = random.Random()
        # 随机读取10w条值不为null的记录，重复操作10此取平均值
        for i in range(10):
            result = []
            for j in range(100000):
                random_index = r.randint(1, 5000000)
                result.append({"index": random_index, "value": tree.get(random_index)})
            print(result)
        tree.close()

    def test_get_status(self):
        tree = BPlusTree.create("test.db", 4096, 1000)
        tree.get_status()
        tree.close()

    def test_del_and_read(self):
        pass


if __name__ == '__main__':
    unittest.main()
