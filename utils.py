def find_last_leq(arr, target):
    """
    在有序数组 arr 中查找小于等于 target 的最后一个最大值的索引。
    如果不存在这样的值，返回 -1。
    """
    left, right = 0, len(arr) - 1
    last_leq_index = -1  # 初始化为 -1，表示未找到满足条件的值

    while left <= right:
        mid = (left + right) // 2
        if arr[mid] <= target:
            last_leq_index = mid  # 更新可能的候选答案
            left = mid + 1  # 在右半部分继续搜索，看是否有更大的符合条件的值
        else:
            right = mid - 1  # 在左半部分搜索

    return last_leq_index