import random
import secrets
import string
import time
import unittest
from main import BPlusTree


class MyTestCase(unittest.TestCase):
    def test_tree(self):
        with BPlusTree.create("test.db", 4096, 100) as tree:
            tree.insert(1, "hello, World!")
            tree.insert(2, "GoodBye, World!")
            tree.insert(3, "ggez!")
            tree.insert(4, "xpl")
            tree.insert(5, "662666668")
            tree.insert(6, "im a long data 66666666666666666666666666666666666")
            tree.insert(7, "你好")
            tree.insert(8, 1234546789)

    def test_insert_data(self):
        """插入500w条数据，id为1到5000000的自增int类型整数，values为随机长度字符串"""
        times = 50000
        records = 100
        with BPlusTree.create("test.db", 4096, 1000) as tree:
            for i in range(times):
                for j in range(records):
                    length = secrets.randbelow(10 - 5 + 1) + 5
                    characters = string.ascii_letters + string.digits
                    secure_random_string = ''.join(secrets.choice(characters) for _ in range(length))
                    tree.insert(i * 100 + j + 1, secure_random_string)
            # 平均耗时1200s，数据库文件100MB+

    def test_read(self):
        capacity = [10,30,50,100,300,500,800,1000,2000,5000]
        result = []
        for index, value in enumerate(capacity):
            with BPlusTree.create("test.db", 4096, value) as tree:
                r = random.Random()
                # 随机读取10w条值不为null的记录，重复操作10此取平均值
                avg_time = 0
                for i in range(10):
                    start_time = time.time()
                    result = []
                    for j in range(100000):
                        random_index = r.randint(1, 5000000)
                        result.append({"index": random_index, "value": tree.get(random_index)})
                    print(result)
                    end_time = time.time()
                    avg_time += end_time - start_time
                print(f"当内存中最大可存放{value}个页面时，平均读取耗时：{round(avg_time / 10)}")
                result[index] = round(avg_time / 10)

        # 最后将所有结果统一打印到控制台
        for index, value in enumerate(capacity):
            print(f"当内存中最大可存放{value}个页面时，平均读取耗时：{result[index]}")

    def test_get_status(self):
        """测试get_status()方法"""
        with BPlusTree.create("test.db", 4096, 1000) as tree:
            print(tree.get_status())

    def test_del_and_read(self):
        """删除数据后，再进行读取，查看此时B+树的效率变化以及节点的合并次数"""
        pass


if __name__ == '__main__':
    unittest.main()
