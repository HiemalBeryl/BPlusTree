import struct
from typing import Optional, List, Union

from utils import find_last_leq


class Node:
    """
    B+树节点（页面）
    Attributes:
        page_count: 计数器，用于产生页面的唯一id
        page_max_size: 页面的大小上限，默认为16kb，超出上限后页面应该主动分裂
        default_merge_size: 页面的合并默认大小，默认为7kb，当两个相邻的页面大小都小于改值时应主动合并
    """
    page_count: int = 0
    page_max_size: int = 16384
    default_merge_size = 8192 - 1024

    def __init__(self, page_parent: Optional[int] = None, is_leaf: bool = False):
        """
        Attributes:
            page_offset: 当前页面唯一id，也是后续B+树在硬盘文件中查询时的偏移量
            page_parent: 当前页面的父节点
            page_prev: 上一个节点
            page_next: 下一个节点
            keys: 记录的主键
            values: 当节点为非叶子节点时，表示key对应的子页面id；当节点为叶子节点时，表示对应的str数据
            is_leaf: 节点类型
        """
        Node.page_count += 1
        self.page_offset = Node.page_count
        self.page_parent: Optional[int] = page_parent
        self.page_prev: Optional[int] = None
        self.page_next: Optional[int] = None
        self.keys: List[int] = []
        self.values: List[Union[int, str]] = []
        self.is_leaf: bool = is_leaf

        self.size: int = len(self.serialize())
        self.is_changed = False

    def serialize(self) -> bytes:
        """
        序列化Node对象到二进制数据。
        """
        # 定义每个字段的格式和大小
        format_str = "=QQQQI"  # page_offset, page_parent, page_prev, page_next 使用Q表示8字节整数，is_leaf 使用I表示4字节整数
        keys_format_str = "i" * len(self.keys)  # 假设keys都是int类型，每个用i表示4字节整数
        values_format_str = "i" if not self.is_leaf else "s" * len(self.values)  # 非叶节点values是int，叶节点是str

        # 计算values部分的格式字符串长度
        if self.is_leaf:
            values_format_str = keys_format_str.replace("i", "s")  # 保持keys和values长度一致，str长度需预估或动态调整

        # 拼接完整的格式字符串
        full_format_str = format_str + keys_format_str + values_format_str

        # 准备数据
        data = [
            self.page_offset,
            self.page_parent or 0,  # None转为0
            self.page_prev or 0,
            self.page_next or 0,
            int(self.is_leaf),
            *self.keys,
        ]

        # 处理values，注意str需要编码为bytes
        if self.is_leaf:
            data.extend(value.encode() for value in self.values)
        else:
            data.extend(self.values)

        # 打包数据为二进制
        serialized_data = struct.pack(full_format_str, *data)

        return serialized_data

    def calculate_size(self, *args) -> int:
        """
        计算插入新数据后，是否超过页面大小上限。
        目前values为str类型时不需要实现此方法计算大小直接在tree的插入方法中实现，后续如果改成dict类型再实现。
        考虑到每次插入后再调用serialize()方法可能会比较耗时，如果超出大小还要再进行回退操作。
        """
        pass

    def split(self, top: Optional['Node'] = None) -> tuple['Node', 'Node', 'Node']:
        """当达到页面大小上限时分裂节点"""
        top = Node() if top is None else top
        right = Node()
        mid = int(len(self.keys) // 2)

        right.keys = self.keys[mid + 1:]
        right.values = self.values[mid + 1:]
        right.page_prev = self.page_offset
        right.page_next = self.page_next

        if len(top.keys) > 0:
            index = find_last_leq(top.keys, right.keys[0])
            top.keys.insert(index, right.keys[0])
            top.values.insert(index, right.page_offset)
        else:
            top.keys = [self.keys[0], right.keys[0]]
            top.values = [self.page_offset, right.page_offset]
            right.page_parent = top.page_offset
            self.page_offset = top.page_offset

        self.keys = self.keys[:mid + 1]
        self.values = self.values[:mid + 1]
        self.page_parent = top.page_offset
        self.page_next = right.page_offset

        top.is_changed = self.is_changed = right.is_changed = True

        return top, self, right


    def merge(self) -> 'Node':
        """当达到页面下限时合并相邻节点"""

    def is_empty(self) -> bool:
        """页面是否存在记录"""
        return len(self.keys) == 0

    def is_root(self) -> bool:
        """页面是否为B+树的根节点"""
        return self.page_parent is None


class LeafNode(Node):
    def __init__(self, page_parent: Optional[int] = None, is_leaf: bool = False):
        super().__init__(page_parent, is_leaf)

    def add(self, key: int, value: str) -> bool:
        """
        在叶子节点中添加键值对。如果键已经存在，则在其关联的值列表中追加值；如果键不存在，通过二分查找找到合适的位置插入新键值对。
        """
        # 使用二分查找确定插入位置，同时检查键是否已存在
        index = self.binary_search(key)

        # 如果键已存在，则不允许插入
        if index < len(self.keys) and self.keys[index] == key:
            return False

        # 键不存在，插入新键值对
        self.keys.insert(index, key)
        self.values.insert(index, value)  # 新建一个列表存储值

        # 检查是否需要分裂节点
        if len(self.serialize()) > Node.page_max_size:
            self.split()

        return True  # 成功插入

    def binary_search(self, target_key: int) -> int:
        """
        在叶子节点的keys中进行二分查找，返回目标键应该插入的位置。
        如果找到相同的键，则返回其索引；如果目标键小于所有键，则返回0；
        如果目标键大于所有键，则返回keys列表的长度。
        """
        low, high = 0, len(self.keys)
        while low < high:
            mid = (low + high) // 2
            mid_val = self.keys[mid]
            if mid_val < target_key:
                low = mid + 1
            else:
                high = mid
        return low

    def split(self, top: Optional['Node'] = None) -> tuple['Node', 'Node', 'Node']:
        top = Node() if top is None else top
        right = LeafNode(top.page_offset, True)
        mid = int(len(self.keys) // 2)

        right.keys = self.keys[mid + 1:]
        right.values = self.values[mid + 1:]
        right.page_prev = self.page_offset
        right.page_next = self.page_next

        if len(top.keys) > 0:
            index = find_last_leq(top.keys, right.keys[0])
            top.keys.insert(index, right.keys[0])
            top.values.insert(index, right.page_offset)
        else:
            top.keys = [self.keys[0], right.keys[0]]
            top.values = [self.page_offset, right.page_offset]
            right.page_parent = top.page_offset
            self.page_offset = top.page_offset

        self.keys = self.keys[:mid + 1]
        self.values = self.values[:mid + 1]
        self.page_parent = top.page_offset
        self.page_next = right.page_offset

        top.is_changed = self.is_changed = right.is_changed = True

        return top, self, right
