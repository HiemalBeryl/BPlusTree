from collections import OrderedDict

ordered_dict = OrderedDict({1: 'steve', 2: 123456})
ordered_dict[3] = "hihihi"
ordered_dict.update({3: "hello"})
print(ordered_dict)
print(4 in ordered_dict)
print(1 in ordered_dict)
ordered_dict.move_to_end(2, last=False)
print(ordered_dict)
p: tuple = ordered_dict.popitem()
print(ordered_dict)
print(type(p[1]))
print(len(ordered_dict))
print(ordered_dict.get(2))