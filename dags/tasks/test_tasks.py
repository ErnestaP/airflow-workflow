def get_strings():
    return ['file1', 'file2', 'file3']

def scale_string(string):
    return [f'${string}_${count}' for count in range(5)]