import os
import struct
from collections import OrderedDict
from typing import List, Any, Dict, Optional, Union, Tuple


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

    def split(self) -> tuple['Node', 'Node', 'Node']:
        """当达到页面大小上限时分裂节点"""
        left = Node()
        right = Node()
        mid = int(len(self.keys) // 2)

        left.parent = right.parent = self
        left.keys = self.keys[:mid]
        left.values = self.values[:mid]
        right.keys = self.keys[mid + 1:]
        right.values = self.values[mid + 1:]

        self.values = [left, right]
        self.keys = [left.keys[0], right.keys[0]]

        # 为每个子节点重新设置父节点
        for child in left.values:
            if isinstance(child, Node):
                child.parent = left
        for child in right.values:
            if isinstance(child, Node):
                child.parent = right

        return self, left, right

    def merge(self) -> 'Node':
        """当达到页面下限时合并相邻节点"""
        pass

    def is_empty(self) -> bool:
        """页面是否存在记录"""
        return len(self.keys) == 0

    def is_root(self) -> bool:
        """页面是否为B+树的根节点"""
        return self.page_parent is None


class LeafNode(Node):
    def __init__(self, page_parent: Optional[int] = None, is_leaf: bool = False):
        super().__init__(page_parent, is_leaf)

    # TODO：不允许插入key相同的数据，同时应该改为二分以提高效率吧
    def add(self, key: int, value: str) -> bool:
        # Insert key and value if keys list is empty
        if not self.keys:
            self.keys.append(key)
            self.values.append([value])
            return
        # Otherwise, search for every key in keys list.
        for i, item in enumerate(self.keys):
            # Key found
            if key == item:
                # Append value in a list of data
                self.values[i].append(value)
                break
            # Key not found && key < item
            elif key < item:
                # Append key and value before item
                self.keys = self.keys[:i] + [key] + self.keys[i:]
                self.values = self.values[:i] + [[value]] + self.values[i:]
                break
            # If we have reached last iteration.
            elif i + 1 == len(self.keys):
                # Append it in last position.
                self.keys.append(key)
                self.values.append([value])
                break

    def split(self) -> tuple['Node', 'Node', 'Node']:
        top = Node()
        right = LeafNode(top.page_offset, True)
        mid = int(len(self.keys) // 2)

        self.page_parent = top

        right.keys = self.keys[mid:]
        right.values = self.values[mid:]
        right.page_prev = self
        right.page_next = self.page_next

        top.keys = [self.keys[0], right.keys[0]]
        top.values = [self, right]

        self.keys = self.keys[:mid]
        self.values = self.values[:mid]
        self.page_next = right

        return top, self, right


class Memorymanagement:
    """简单的缓存管理，利用有序列表实现LRU策略，所有页面保存在此，能够缓存的最大页面数在初始化时手动设定"""
    def __init__(self, file, capacity: int):
        """
        初始化内存管理器，设定缓存容量。

        :param capacity: 缓存的最大容量，单位为页面数量。
        """
        self.file = file
        self.capacity = capacity
        self.cache = OrderedDict()

    def get_page(self, page_id: int) -> Optional[Node]:
        """
        从缓存中获取页面，若不存在则返回None。
        此操作会将页面移到最近使用的队列尾部。

        :param page_id: 页面ID。
        :return: 页面对象，如果不存在则从磁盘读取。
        """
        if page_id in self.cache:
            # 将访问的页面移到队列末尾，表示最近访问
            self.cache.move_to_end(page_id)
            return self.cache[page_id]
        n = self.read_from_disk(page_id)
        self.put_page(page_id, n)
        return n

    def put_page(self, page_id: int, page: Node) -> None:
        """
        将页面放入缓存。如果缓存已满，则依据LRU策略淘汰最老的页面。

        :param page_id: 页面ID。
        :param page: 页面对象。
        """
        if len(self.cache) >= self.capacity:
            # 缓存已满，淘汰最老的页面
            self.cache.popitem(last=False)  # last=False表示移除最老的项
        self.cache[page_id] = page

    def exists(self, page_id: int) -> bool:
        """
        判断页面ID是否存在于缓存中。

        :param page_id: 页面ID。
        :return: 存在与否的布尔值。
        """
        return page_id in self.cache

    def evict_least_recently_used(self) -> None:
        """
        强制淘汰最老的页面，即使缓存未满时也可调用。
        """
        if self.cache:
            self.cache.popitem(last=False)

    def clear(self) -> None:
        """
        清空整个缓存。
        """
        self.cache.clear()

    def read_from_disk(self, page_id: int) -> Node:
        """
        从磁盘中读取页面，加载到缓存中。

        :param page_id: 页面ID。
        :return: 实例化的Node或LeafNode对象。
        """
        # 跳过前16KB的metadata
        self.file.seek(16384)

        # 计算页面在文件中的偏移量
        page_offset = 16384 + (page_id - 1) * Node.page_max_size

        # 移动文件指针到指定位置
        self.file.seek(page_offset)

        # 读取页面数据
        raw_data = self.file.read(Node.page_max_size)

        # 反序列化数据
        format_str = "=QQQQI"  # 与serialize方法中的格式匹配
        keys_format_str = "i" * Node.page_max_size  # 简化假设，实际情况可能需要更复杂的解析逻辑
        values_format_str = "i"  # 同样简化假设

        full_format_str = format_str + keys_format_str + values_format_str
        try:
            unpacked_data = struct.unpack(full_format_str, raw_data)
        except struct.error:
            raise ValueError(f"Error reading page at id {page_id}, possibly due to corrupted data.")

        # 解析unpacked_data来实例化Node或LeafNode
        # 注意：此处的解析逻辑需要根据实际的serialize方法来调整，以下仅为示例
        page_offset, page_parent, page_prev, page_next, is_leaf_flag, *rest = unpacked_data
        keys = rest[:len(rest) // 2]  # 简单分割keys和values，实际逻辑可能更复杂
        values = rest[len(rest) // 2:]

        if is_leaf_flag:
            node = LeafNode(page_parent, True)
        else:
            node = Node(page_parent, False)

        node.page_offset = page_offset
        node.page_prev = page_prev
        node.page_next = page_next
        node.keys = keys
        node.values = values  # 注意：对于非叶节点，values是子节点的page_id，需要进一步处理

        return node


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

    def create(self, filename: str, page_size: int, capacity: int) -> Memorymanagement:
        """
        创建一个新的B+树文件或打开已存在的文件。

        :param filename: 文件名。
        :param page_size: 页面大小。
        """
        # 检查文件是否存在
        if os.path.exists(filename):
            self.file = open(filename, 'r+b')  # 读写二进制模式打开
            # 这里可以添加读取metadata的逻辑，如果需要的话
        else:
            self.file = open(filename, 'w+b')  # 读写二进制模式创建新文件
            # 初始化metadata，例如写入一些默认值或空值占位
            # 注意：这里省略了写入metadata的具体实现，你需要根据实际需求定义metadata的结构和写入逻辑

        return Memorymanagement(self.file, capacity)

    def close(self, filename: str) -> bool:
        """
        关闭已打开的B+树文件。

        :param filename: 文件名，虽然参数传入但实际操作基于类的成员变量。
        """
        if hasattr(self, 'file') and self.file is not None:
            self.file.close()
            self.file = None  # 置为None以避免后续错误操作
            return True
        return False

    def get(self, key: int) -> Union['Node', str]:
        pass

    def insert(self, key: str, value: str):
        pass

    def delete(self, key: int) -> bool:
        pass

    def get_status(self):
        print(self.__str__())
