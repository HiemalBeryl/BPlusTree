import os
import struct
from collections import OrderedDict
from typing import List, Optional, Union, Tuple

from memory import Memorymanagement
from node import Node, LeafNode
from utils import find_last_leq


class BPlusTree:
    def __init__(self, **kwargs):
        self.root_page_id = kwargs.get("root_page_id", 1)  # 根节点位置
        self.page_size = kwargs.get("page_size", 0)  # 页面大小
        self.fill_rate = kwargs.get("fill_rate", 0.0)  # 填充率，即节点已用空间与总空间的比例
        self.height = kwargs.get("height", 0)  # 树的高度或层数
        self.node_count = kwargs.get("node_count", 0)  # 节点总数
        self.split_count = kwargs.get("split_count", 0)  # 分裂次数
        self.merge_count = kwargs.get("merge_count", 0)  # 合并次数
        self.max_page_count = kwargs.get("max_page_count", 0)  # 最大页面id
        self.empty_page_count = kwargs.get("empty_page_count", [])  # 空闲页面id
        self.filename = kwargs.get("filename", None)

        self.root_node: Optional[Node] = kwargs.get("root_node", None)
        self.memory: Optional[Memorymanagement] = kwargs.get("memory", None)

        self.memory.write_metadata(
            root_page_id=self.root_page_id,
            page_size=self.page_size,
            fill_rate=self.fill_rate,
            height=self.height,
            node_count=self.node_count,
            split_count=self.split_count,
            merge_count=self.merge_count,
            max_page_count=self.max_page_count,
            empty_page_count=self.empty_page_count,
            filename=self.filename
        )

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()

    @staticmethod
    def create(filename: str, page_size: int, capacity: int) -> 'BPlusTree':
        """
        创建一个新的B+树文件或打开已存在的文件。

        :param filename: 文件名。
        :param page_size: 页面大小。
        """
        # 检查文件是否存在
        d: Optional[dict] = None
        if os.path.exists(filename):
            memory = Memorymanagement(filename, capacity)
            d = memory.read_metadata()
            print(d)
            if d is None:
                d = {
                    'root_page_id': 1,
                    'page_size': 16384,
                    'root_node': LeafNode(is_leaf=True),
                    'node_count': 1
                }
            rid = d.get('root_page_id')
            assert rid is not None
            root_node = memory.get_page(rid)
            d['root_node'] = root_node
            d['memory'] = memory
            d['filename'] = filename
        else:
            with open(filename, 'w+b') as file:  # 读写二进制模式创建新文件
                pass
            memory = Memorymanagement(filename, capacity)
            root_node = LeafNode(is_leaf=True)
            d = {
                'root_page_id': 1,
                'page_size': 16384,
                'root_node': root_node,
                'memory': memory,
                'filename': filename
            }
            memory.write_to_disk(root_node)

        return BPlusTree(**d)

    def close(self) -> bool:
        """关闭已打开的B+树文件。"""
        print("prepare to close")

        self.memory.clear()
        self.memory.write_metadata(
            root_page_id=self.root_page_id,
            page_size=self.page_size,
            fill_rate=self.fill_rate,
            height=self.height,
            node_count=self.node_count,
            split_count=self.split_count,
            merge_count=self.merge_count,
            max_page_count=self.max_page_count,
            empty_page_count=self.empty_page_count,
            filename=self.filename
        )
        return True

    def get(self, key: int) -> Optional[str]:
        node = self.root_node
        while True:
            index = find_last_leq(node.keys, key)
            if node.is_leaf:
                if node.keys[index] == key:
                    return node.values[index]
                else:
                    return None
            else:
                node = self.memory.get_page(node.values[index])


    def insert(self, key: int, value: str):
        if self.root_node is None:
            self.root_node = LeafNode(is_leaf=True)
            self.root_node.keys.append(key)
            self.root_node.values.append(value)
            self.memory.put_page(self.root_node.page_offset, self.root_node)
        else:
            # 1. 首先找到叶子节点，向叶子节点中插入数据
            node = self.root_node
            while node.is_leaf is False:
                try:
                    i = find_last_leq(node.keys, key)
                    node = self.memory.get_page(node.values[i])
                except Exception as e:
                    print(i)
                    print(node.keys)
                    print(node.values)
                    print(node.page_offset)
                    print(e.with_traceback)
            # 1.1 相同主键的id只允许存在一条，如果重复插入则覆盖前面的数据
            if key in node.keys:
                index = node.keys.index(key)
                node.values[index] = value
                node.is_changed = True
                self.memory.put_page(node.page_offset, node)
            else:
                index = find_last_leq(node.keys, key) + 1
                node.keys.insert(index, key)
                node.values.insert(index, value)
                node.is_changed = True
                self.memory.put_page(node.page_offset, node)
            # 2. 如果叶子节点插入后已满，则分裂节点
            if len(node.serialize()) <= Node.page_max_size:
                return True
            else:
                node_is_leaf = True
                while True:
                    p, l, r = self.__split_node(node)
                    self.split_count += 1
                    if p.page_parent is None or p.page_parent == 0:
                        self.root_page_id = p.page_offset
                        self.root_node = p
                    if not node_is_leaf:
                        p.is_leaf = l.is_leaf = r.is_leaf = False
                    self.memory.put_page(p.page_offset, p)
                    self.memory.put_page(l.page_offset, l)
                    self.memory.put_page(r.page_offset, r)
                    # 3. 父节点在此时插入了右孩子节点的最小值，如果此时父节点已满，则需要循环向上分裂父节点
                    if len(p.serialize()) <= Node.page_max_size:
                        break
                    node = p
                    node_is_leaf = False

    def delete(self, key: int) -> int:
        """删除页面中指定键值的数据。返回删除条数（0或1）"""
        node = self.root_node
        while node.is_leaf is False:
            node = self.memory.get_page(node.values[find_last_leq(node.keys, key)])
        index = find_last_leq(node.keys, key)
        # 在叶子节点中不存在要删除的数据，返回0
        if node.keys[index] != key:
            return 0
        else:
            node.keys.pop(index)
            node.values.pop(index)
            # 叶节点删除记录之后没有处于半满状态需要合并相邻节点或者重新分配
            if len(node.serialize()) < Node.default_merge_size:
                self.__coalesce_or_redistribute(node)

        node.is_changed = True
        self.memory.put_page(node.page_offset, node)
        return 1

    def get_status(self):
        with open(self.filename, 'rb') as file:
            self.page_size = Node.page_max_size
            # 随机找一个数，看获取多少次页面可以走到根节点即可得到树高
            node = self.memory.get_page(4869)
            height = 0
            while not node.page_offset == self.root_page_id:
                height += 1
                node = self.memory.get_page(node.page_parent)
            self.height = height + 1

            if self.root_node.is_leaf:
                self.fill_rate = round(len(self.root_node.serialize()) / Node.page_max_size, 4)
            else:
                page_offsets = [self.root_node.page_offset]
                while len(page_offsets) > 0:
                    page_offset = page_offsets.pop(0)
                    node = self.memory.get_page(page_offset)
                    if node.is_leaf:
                        self.fill_rate += Node.page_max_size - len(node.serialize())
                    else:
                        page_offsets.extend(node.values)
                self.fill_rate = round(self.fill_rate / (os.path.getsize(self.filename) - 16384), 4)
        return self.__dict__

    def __coalesce_or_redistribute(self, node) -> bool:
        if node.is_root():
            return self.__adjust_root(node)

        # 找到相邻的兄弟节点
        brother = None
        if node.page_prev is not None or node.page_next is not None:
            if node.page_prev is not None or not node.page_prev <= 0:
                brother = self.memory.get_page(node.page_prev)
                if not node.page_parent == brother.page_parent:
                    brother = None
            elif node.page_next is not None or not node.page_next <= 0:
                brother = self.memory.get_page(node.page_next)
                if not node.page_parent == brother.page_parent:
                    brother = None
            else:
                pass

        if brother is not None:
            # 如果两个节点的大小和大于 max_size，就直接重新分配，否则直接合并兄弟节点
            is_merge = len(node.serialize()) + len(brother.serialize()) <= Node.page_max_size
            if is_merge:
                self.__coalesce(node, brother)
            else:
                self.__redistribute(node, brother)

            node.is_changed = True
            brother.is_changed = True
            self.memory.put_page(node.page_offset, node)
            self.memory.put_page(brother.page_offset, brother)

            return is_merge
        return False

    def __adjust_root(self, old_root_node: Node) -> bool:
        """
        此函数处理根节点的合并过程，包括下面两种情况
        1. 根节点经过删除操作后，根节点只有一个孩子节点，则直接调整根节点为合并后的叶子节点
        2. 根节点就是叶节点，内部不存在记录
        """
        is_deleted: bool = False

        # 根节点只包含一个无效键时需要删除根节点，将子节点变为根节点；根节点为叶节点且为没有键值对时，删除整棵树
        if old_root_node.is_leaf and len(old_root_node.keys) == 1:
            child = self.memory.get_page(old_root_node.values[0])
            if isinstance(child, Node):
                # 将child中的属性复制到root中
                page_index = old_root_node.page_offset
                old_root_node.page_parent = None
                old_root_node.page_prev = child.page_prev
                old_root_node.page_next = child.page_next
                old_root_node.keys = child.keys
                old_root_node.values = child.values
                old_root_node.is_leaf = child.is_leaf
                old_root_node.is_changed = True
        elif old_root_node.is_leaf and len(old_root_node.keys) == 0:
            is_deleted = True

        return is_deleted

    def __coalesce(self, node: Node, brother: Node) -> bool:
        # 如果兄弟节点在右边，需要交换两个指针的值，这样就能确保数据移动方向是从右到左
        l_node, r_node = (brother, node) if node.page_next == brother.page_offset else (node, brother)
        assert l_node.page_parent == r_node.page_parent
        p_node = self.memory.get_page(l_node.page_parent)

        # 内部节点要从父节点获取插到 node 中的键，右兄弟节点对应的是第一个有效键，左兄弟节点对应的就是 index - 1 处的键
        assert len(l_node.serialize()) + len(r_node.serialize()) <= Node.page_max_size
        r_index = find_last_leq(p_node.values, r_node.page_offset)

        # 将键值对移动到兄弟节点之后删除节点
        l_node.keys.extend(r_node.keys)
        l_node.values.extend(r_node.values)
        l_node.page_next = r_node.page_next
        # TODO: deletePage

        # 删除父节点中的键值对，并递归调整父节点
        p_node.keys.pop(r_index)
        p_node.values.pop(r_index)
        self.memory.put_page(p_node.page_offset, p_node)
        self.merge_count += 1
        return self.__coalesce_or_redistribute(p_node)

    def __redistribute(self, node: Node, brother: Node) -> bool:
        """将兄弟节点的一个键值对移动到 node 中"""
        # 更新父节点
        p_node = self.memory.get_page(node.page_parent)

        # 内部节点要从父节点获取插到 node 中的键，右兄弟节点对应的是第一个有效键，左兄弟节点对应的就是 index - 1 处的键
        index = find_last_leq(p_node.values, brother.page_offset)

        # 兄弟节点在右边，移动第一个键值对给 node，否则将兄弟节点的最后一个键值对移给 node 并更新父节点的键
        if p_node.keys[index + 1] == node.keys[0]:
            # 说明node在brother的右边
            node.keys.insert(0, brother.keys.pop(-1))
            node.values.insert(0, brother.values.pop(-1))
            p_node.keys[index + 1] = node.keys[0]
        elif p_node.keys[index - 1] == node.keys[0]:
            # 说明node在brother的左边
            node.keys.append(brother.keys.pop(0))
            node.values.append(brother.values.pop(0))
            p_node.keys[index] = brother.keys[0]

        assert len(node.serialize()) <= Node.page_max_size
        assert len(brother.serialize()) <= Node.page_max_size
        assert len(node.keys) >= 1
        self.memory.put_page(node.page_offset, node)
        self.memory.put_page(brother.page_offset, brother)
        return True

    def __split_node(self, node: Node) -> Tuple[Node, Node, Node]:
        if node.is_leaf:
            right = LeafNode(is_leaf=True)
        else:
            right = Node()
        self.node_count += 1
        if node.page_offset == self.root_page_id:
            top = Node()
            self.node_count += 1
        else:
            top = self.memory.get_page(node.page_parent)


        mid = int(len(node.keys) // 2)

        right.keys = node.keys[mid:]
        right.values = node.values[mid:]
        right.page_prev = node.page_offset
        right.page_next = node.page_next
        right.page_parent = top.page_offset

        if len(top.keys) > 0:
            index = find_last_leq(top.keys, right.keys[0])
            top.keys.insert(index + 1, right.keys[0])
            top.values.insert(index + 1, right.page_offset)
        else:
            top.keys = [node.keys[0], right.keys[0]]
            top.values = [node.page_offset, right.page_offset]


        node.keys = node.keys[:mid]
        node.values = node.values[:mid]
        node.page_parent = top.page_offset
        node.page_next = right.page_offset

        top.is_changed = node.is_changed = right.is_changed = True

        return top, node, right
