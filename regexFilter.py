import re

#logentry = """loglevel=INFO  class=c.s.k.j.b.v.a.t.o.TripObserver message=Analytics#d280a015-0da6-4a33-86b5-bccd094d1ce4#8504100#8503000#[8507000, 8500210]#2024-09-07T16:26#2024-09-06T16:05#null#[{"value":"8504100|8507000|8500207|8500218|8502113|8503000","count":2},{"value":"8504100|8507000|8576917|8500210|8500218|8502113|8503000","count":2},{"value":"8504100|8507000|8500207|8500218|8503000","count":1},{"value":"8504100|8507000|8500218|8500207|8503000","count":1}]"""
def extract_data(log_line):
    # Extract the data from the log line
    match = re.search(r'Analytics#(.+?)(?:\s|$)', log_line)
    if match:
        return match.group(1)
    else:
        return None