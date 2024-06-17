import json
import struct
from collections import OrderedDict
from json import JSONDecodeError
from typing import Optional

from node import Node, LeafNode


class Memorymanagement:
    """简单的缓存管理，利用有序列表实现LRU策略，所有页面保存在此，能够缓存的最大页面数在初始化时手动设定"""

    #TODO: 弄清楚ordereddict是如何组织数据的，ai生成的好像不对，page——id不等于下标
    def __init__(self, filename: str, capacity: int):
        """
        初始化内存管理器，设定缓存容量。

        :param capacity: 缓存的最大容量，单位为页面数量。
        """
        self.filename = filename
        self.capacity = capacity
        self.cache = OrderedDict()
        self.empty_page_count = []  # 空闲页面id

    def get_page(self, page_id: int) -> Optional[Node]:
        """
        从缓存中获取页面，若不存在则从磁盘中加载或新建页面。
        此操作会将页面移到最近使用的队列尾部。

        :param page_id: 页面ID。
        :return: 页面对象，如果不存在则从磁盘读取。
        """
        if page_id in self.cache:
            # 将访问的页面移到队列末尾，表示最近访问
            self.cache.move_to_end(page_id)
            return self.cache[page_id]
        n = self.read_from_disk(page_id)
        if n is not None:
            self.put_page(page_id, n)
        return n

    def put_page(self, page_id: int, page: Node, **kwargs) -> None:
        """
        将页面放入缓存。如果缓存已满，则依据LRU策略淘汰最老的页面。

        :param page_id: 页面ID。
        :param page: 页面对象。
        """
        # 先检查缓存中是否已有对应id的页面，已存在则对其进行更新并移到队尾
        if page_id in self.cache:
            self.cache.update({page_id: page})
            self.cache.move_to_end(page_id)
            return None
        if len(self.cache) >= self.capacity:
            # 缓存已满，淘汰最老的页面，将其持久化到磁盘中
            popitem = self.cache.popitem(last=False)
            if isinstance(popitem[1], Node):
                if popitem[1].is_changed:
                    self.write_to_disk(popitem[1])
            else:
                print("还是不行")
                print(type(popitem[1]))
        self.cache[page_id] = page

    def evict_least_recently_used(self) -> None:
        """
        强制淘汰最老的页面，即使缓存未满时也可调用。
        """
        if self.cache:
            popitem = self.cache.popitem(last=False)
            if isinstance(popitem[1], Node):
                if popitem[1].is_changed:
                    self.write_to_disk(popitem[1])

    def clear(self) -> None:
        """
        清空整个缓存。
        """
        for item in self.cache.values():
            if isinstance(item, Node):
                if item.is_changed:
                    self.write_to_disk(item)
        self.cache.clear()

    def read_from_disk(self, page_id: int) -> Node:
        """
        从磁盘中读取页面，加载到缓存中。

        :param page_id: 页面ID。
        :return: 实例化的Node或LeafNode对象。
        """
        # 判断page_id是否合法
        if page_id is None or int(page_id) <= 0:
            raise ValueError("Page ID must be greater than 0.")

        # 计算页面在文件中的偏移量，16384为metadata固定偏移量
        page_offset = 16384 + ((page_id - 1) * Node.page_max_size)

        with open(self.filename, "rb") as file:
            # 移动文件指针到指定位置
            file.seek(page_offset)

            # 读取页面数据
            raw_data = file.read(Node.page_max_size)

        # 反序列化数据
        format_str = "=QQQQIQ"  # 与serialize方法中的格式匹配

        # 首先读取节点的头部数据，再根据节点类型返回去读记录数据
        try:
            meta_data = struct.unpack(format_str, raw_data[:5 * 8 + 4 * 1])
            page_offset, page_parent, page_prev, page_next, is_leaf, records_size = meta_data
            raw_data = raw_data[5 * 8 + 4 * 1:]
            keys_format_str = "i" * records_size
            keys = list(struct.unpack(keys_format_str, raw_data[:records_size * 4]))
            raw_data = raw_data[records_size * 4:]
            values = []
            binary_data = raw_data.split(b"\x00")
            for b in binary_data:
                if b:
                    values.append(b.decode("utf-8"))
                    continue
                break

        except struct.error as e:
            print(e.with_traceback)
            raise ValueError(f"Error reading page at id {page_id}, possibly due to corrupted data.")

        # 解析unpacked_data来实例化Node或LeafNode
        # 注意：此处的解析逻辑需要根据实际的serialize方法来调整，以下仅为示例

        if is_leaf:
            node = LeafNode(int(page_parent), True)
            node.values = values
        else:
            node = Node(int(page_parent), False)
            node.values = [int(value) for value in values]

        node.page_offset = int(page_offset)
        node.page_parent = int(page_parent)
        node.page_prev = int(page_prev)
        node.page_next = int(page_next)
        node.is_leaf = bool(is_leaf)
        node.size = int(records_size)
        node.keys = keys

        return node

    def write_to_disk(self, page) -> bool:
        serialize: bytes = page.serialize()
        # 计算页面在文件中的偏移量，16384为metadata固定偏移量
        page_offset = 16384 + ((page.page_offset - 1) * Node.page_max_size)

        # 移动文件指针到指定位置并写入数据
        with open(self.filename, "r+b") as file:
            file.seek(page_offset)
            file.write(serialize)
        return True

    def write_metadata(self, **kwargs) -> bool:
        # 将kwargs转换成bytes
        empty: bytes = b"\x00" * 2000
        serialize: bytes = json.dumps(kwargs).encode('utf-8')

        # 计算页面在文件中的偏移量，16384为metadata固定偏移量
        page_offset = 0

        # 移动文件指针到指定位置并写入数据
        with open(self.filename, "r+b") as file:
            file.seek(page_offset)
            file.write(empty)
            file.seek(page_offset)
            file.write(serialize)
        return True

    def read_metadata(self) -> Optional[dict]:
        # 计算页面在文件中的偏移量，16384为metadata固定偏移量
        page_offset = 0

        with open(self.filename, "rb") as file:
            # 移动文件指针到指定位置
            file.seek(page_offset)

            # 读取页面数据
            raw_data = file.read(16384).split(b"\x00")[0]
        try:
            s = raw_data.decode('utf-8')
            s = s[:s.find("}") + 1]
            j = json.loads(s)
            return j
        except JSONDecodeError as e:
            print(e.with_traceback())
            return None
