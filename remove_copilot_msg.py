def message_callback(msg):
    import re
    s = msg.decode('utf-8')
    # Remove any Co-authored-by lines that reference Copilot
    s = re.sub(r'(?im)^\s*Co-authored-by:.*Copilot.*\n?', '', s)
    return s.encode('utf-8')
