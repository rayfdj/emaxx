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
    thread = frame.GetThread()
    location = None
    for f in thread:
        if 'scan_words' in (f.GetFunctionName() or ''):
            value = f.FindVariable('address')
            if value.GetError().Success():
                location = value.GetValueAsUnsigned() - 8
    if location:
        for f in thread:
            if f.GetSP() <= location <= f.GetFP():
                print('ROOT_OWNER', hex(location), f, flush=True)
                for value in f.GetVariables(True, True, True, True):
                    print('LOCAL', value.GetName(), value, flush=True)
                print(f.Disassemble(), flush=True)
    return False
