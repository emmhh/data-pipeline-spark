import os
import random
from datetime import datetime, timedelta

def generate_random_datetime(year_specified):
    start_date = datetime(year=year_specified, month=1, day=1)
    end_date = datetime(year=year_specified, month=12, day=31, hour=23, minute=59, second=59)

    total_seconds = int((end_date - start_date).total_seconds())
    random_seconds = random.randint(0, total_seconds)

    random_datetime = start_date + timedelta(seconds=random_seconds)

    return random_datetime

def generate_random_floats(n):
    random_floats = [random.random() for _ in range(n)]

    formatted_string = ','.join(f'{num:.8f}' for num in random_floats) + '\n'

    return formatted_string

with open('./tmp.csv', 'r') as f:
    lines = f.readlines()

for patient in range(1, 100):
    year = random.randint(2010, 2024)
    indexes = random.randint(1, 20)
    test_time = generate_random_datetime(year)
    test_time += timedelta(seconds=random.randint(1, 1200))
    for index in range(1, indexes):
        num = random.randint(600, 28800)
        file_name = f'PUH-{year}-{str(patient).zfill(3)}_{str(index).zfill(2)}.csv'
        line_0 = lines[0].replace('File,', f'File,{file_name}')
        line_4 = lines[4].replace('TestDate,', f'TestDate,{test_time.year}/{test_time.month}/{test_time.day}')
        line_5 = lines[5].replace('TestTime,', f'TestTime,{test_time.hour}:{test_time.minute}:{test_time.second}')
        with open(f'./tmp/{file_name}', 'w') as f:
            f.writelines(line_0)
            f.writelines(lines[1])
            f.writelines(lines[2])
            f.writelines(lines[3])
            f.writelines(line_4)
            f.writelines(line_5)
            f.writelines(lines[6])
            f.writelines(lines[7])
            start_time = int(test_time.timestamp())
            for i in range(num):
                line = f'{(start_time + i) / 1e5},{i},'
                line += generate_random_floats(6037)
                f.writelines(line)
        test_time += timedelta(seconds=num)
        test_time += timedelta(seconds=random.randint(1, 600))
    break
