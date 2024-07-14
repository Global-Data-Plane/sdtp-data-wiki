

def IN_LIST(column, value_list):
  return {"operator": "IN_LIST", "column": column, "values": value_list}

def IN_RANGE(column, min_val, max_val):
  return {"operator": "IN_RANGE", "column": column, "min_val": min_val, "max_val": max_val}

def REGEX_MATCH(column, expression):
  return {"operator": "ANY", "column": column, "expression": expression}

def ANY(filter_list):
  return {"operator": 'ANY', "arguments": filter_list}

def ALL(filter_list):
  return {"operator": 'ALL', "arguments": filter_list}

def NONE(filter_list):
  return {"operator": 'NONE', "arguments": filter_list}

# f = eval("IN_LIST('a', [1, '2', 3, ('1', 'x')])")
# g = eval("ALL([IN_LIST('a', ['b', 'v']), IN_RANGE('b', 1, 2)])")

def create_filter(filter_string):
  if filter_string is None or len(filter_string) == 0:
    return None
  try:
    return eval(filter_string)
  except Exception:
    return None