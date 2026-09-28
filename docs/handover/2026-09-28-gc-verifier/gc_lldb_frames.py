import lldb
counter = 0
def capture(frame, bp_loc, internal_dict):
    global counter
    counter += 1
    print('ROOT_BREAK', counter, flush=True)
    for name in ('stage', 'key', 'marked', 'root'):
        value = frame.FindVariable(name)
        print('VALUE', name, value, flush=True)
    for f in frame.GetThread():
        print('FRAME', f.GetFrameID(), hex(f.GetSP()), hex(f.GetFP()), f.GetFunctionName(), f.GetLineEntry(), flush=True)
        if 'scan_words' in (f.GetFunctionName() or ''):
            for name in ('low', 'high', 'address', 'word'):
                print('SCAN', name, f.FindVariable(name), flush=True)
    return False
