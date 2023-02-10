list = []
for i in range(10):
    if i % 2 == 0:
        if i < 10:
            list.append(i**2)
        else:
            list.append(i*2)

print(list)
