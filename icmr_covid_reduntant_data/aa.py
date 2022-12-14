# import datetime

# x = '2022-02-28 00:00:00'

# x = datetime.datetime.strptime(x, "%Y-%m-%d %H:%M:%S")

# x += datetime.timedelta(days=1)
# print(x)
# print(type(str(x)))


b = {"a":0, "b":2}

# print(b.get('ax'))

try:
    if b.get('ab') == 0:
        print('dpne')
    else:
        print('elseeeeee')

except Exception as e:
    print(e)
