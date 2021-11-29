def remove_prefix(file_name):
    file_name_parts_without_prefix = file_name.split('.')[:-1]
    file_name_without_prefix = ('.').join(file_name_parts_without_prefix)
    return file_name_without_prefix