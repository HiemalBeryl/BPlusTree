import os
import struct
from collections import OrderedDict
from typing import List, Optional, Union

from memory import Memorymanagement
from node import Node, LeafNode
from utils import find_last_leq


class BPlusTree:
    def __init__(self):
        self.root_page_id = 0  # 根节点位置
        self.page_size = 0  # 页面大小
        self.max_keys_per_node = 0  # 每个节点最大键值数
        self.fill_rate = 0.0  # 填充率，即节点已用空间与总空间的比例
        self.height = 0  # 树的高度或层数
        self.node_count = 0  # 节点总数
        self.split_count = 0  # 分裂次数
        self.merge_count = 0  # 合并次数

        self.file = None
        self.root_node: Optional[Node] = None
        self.memory: Optional[Memorymanagement] = None

    def create(self, filename: str, page_size: int, capacity: int) -> Memorymanagement:
        """
        创建一个新的B+树文件或打开已存在的文件。

        :param filename: 文件名。
        :param page_size: 页面大小。
        """
        # 检查文件是否存在
        if os.path.exists(filename):
            self.file = open(filename, 'r+b')  # 读写二进制模式打开
            # TODO： 这里可以添加读取metadata的逻辑，如果需要的话
        else:
            self.file = open(filename, 'w+b')  # 读写二进制模式创建新文件
            # TODO： 初始化metadata，例如写入一些默认值或空值占位

        self.memory = Memorymanagement(self.file, capacity)
        return self.memory

    def close(self, filename: str) -> bool:
        """
        关闭已打开的B+树文件。

        :param filename: 文件名，虽然参数传入但实际操作基于类的成员变量。
        """
        if hasattr(self, 'file') and self.file is not None:
            self.file.close()
            self.file = None
            return True
        return False

    def get(self, key: int) -> Optional[str]:
        node = self.root_node.keys
        while True:
            index = find_last_leq(node, key)
            node = self.memory.get_page(self.root_node.values[index])
            if node.is_leaf:
                r = find_last_leq(node.keys, key)
                if r == key:
                    return node.values[r]
                else:
                    return None

    def insert(self, key: int, value: str):
        if self.root_node is None:
            self.root_node = LeafNode(is_leaf=True)
            self.root_node.keys.append(key)
            self.root_node.values.append(value)
        else:
            # 1. 首先找到叶子节点，向叶子节点中插入数据
            node = self.root_node
            while node.is_leaf is False:
                node = self.memory.get_page(self.root_node.values[find_last_leq(node.keys, key)])
            index = find_last_leq(node.keys, key)
            node.keys.insert(index, key)
            node.values.insert(index, value)
            # 2. 如果叶子节点插入后已满，则分裂节点
            if len(node.serialize()) <= Node.page_max_size:
                return True
            else:
                while True:
                    p, l, r = node.split(self.memory.get_page(node.page_parent))
                    self.split_count += 1
                    self.memory.put_page(p.page_offset, p)
                    self.memory.put_page(l.page_offset, l)
                    self.memory.put_page(r.page_offset, r)
                    # 3. 父节点在此时插入了右孩子节点的最小值，如果此时父节点已满，则需要循环向上分裂父节点
                    if len(p.serialize()) <= Node.page_max_size:
                        break
                    node = p

    def delete(self, key: int) -> int:
        """删除页面中指定键值的数据。返回删除条数（0或1）"""
        node = self.root_node
        while node.is_leaf is False:
            node = self.memory.get_page(self.root_node.values[find_last_leq(node.keys, key)])
        index = find_last_leq(node.keys, key)
        # 在叶子节点中不存在要删除的数据，返回0
        if node.keys[index] != key:
            return 0
        else:
            node.keys.pop(index)
            node.values.pop(index)
            # 判断是否需要进行平衡操作，如果需要，则需要向兄弟节点借用数据并循环向上进行平衡操作
            while len(node.serialize()) < Node.default_merge_size:
                # 1. 向兄弟节点借用记录，直到自己的大小大于默认值
                brother = self.memory.get_page(node.page_prev)
                parent = self.memory.get_page(node.page_parent)
                flag = 0
                if isinstance(brother, Node):
                    if len(brother.serialize()) > Node.default_merge_size:
                        while len(node.serialize()) < Node.default_merge_size:
                            k, v = brother.keys.pop(), brother.values.pop()
                            node.keys.insert(0, k)
                            node.values.insert(0, v)
                            if len(brother.serialize()) <= Node.default_merge_size:
                                # 兄弟节点不允许借用，回退本次操作，同时将合并flag标记为1
                                flag = 1
                                brother.keys.insert(len(brother.keys), k)
                                brother.values.insert(len(brother.values), v)
                                node.keys.pop(0)
                                node.values.pop(0)
                                break
                        node.is_changed = True
                        brother.is_changed = True
                        self.memory.put_page(brother.page_offset, brother.page_offset)
                        self.memory.put_page(node.page_offset, node)
                    else:
                        #2. 向兄弟节点借记录失败，则需要合并节点
                        flag = 1
                    # 3. 检查合并标记，进行节点的合并操作
                    if flag == 1:
                        pass

                else:
                    # 兄弟节点不存在
                    pass


        return 1

    def get_status(self):
        print(self.__str__())
