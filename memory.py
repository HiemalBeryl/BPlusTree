import struct
from collections import OrderedDict
from typing import Optional

from node import Node, LeafNode


class Memorymanagement:
    """简单的缓存管理，利用有序列表实现LRU策略，所有页面保存在此，能够缓存的最大页面数在初始化时手动设定"""
    #TODO: 弄清楚ordereddict是如何组织数据的，ai生成的好像不对，page——id不等于下标
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
        #先检查缓存中是否已有对应id的页面，已存在则对其进行更新并移到队尾
        if self.exists(page_id):
            self.cache.move_to_end(page_id)
            self.cache[self.capacity - 1] = page
            return None
        if len(self.cache) >= self.capacity:
            # 缓存已满，淘汰最老的页面，将其持久化到磁盘中
            popitem = self.cache.popitem(last=False)
            if isinstance(popitem, Node):
                if popitem.is_changed:
                    self.write_to_disk(popitem)
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
            popitem = self.cache.popitem(last=False)
            if isinstance(popitem, Node):
                if popitem.is_changed:
                    self.write_to_disk(popitem)

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
        if page_id is None or page_id <= 0:
            return None
            raise ValueError("Page ID must be greater than 0.")

        # 计算页面在文件中的偏移量，16384为metadata固定偏移量
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

    def write_to_disk(self, page) -> bool:
        serialize: bytes = page.serialize()
        # 计算页面在文件中的偏移量，16384为metadata固定偏移量
        page_offset = 16384 + (page.page_offset - 1) * Node.page_max_size

        # 移动文件指针到指定位置并写入数据
        self.file.seek(page_offset)
        self.file.write(serialize)
        return True
