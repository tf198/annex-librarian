import base64

def parse_meta_log(lines):
    result = {}
    field = None

    for line in lines:
        parts = line.split()

        for token in parts[1:]:
            if token[0] in '+-':
                op = token[0]
                token = token[1:]
                if token[0] == '!':
                    token = base64.b64decode(token[1:])

                if op == '+':
                    result[field].add(token)
                else:
                    try:
                        result[field].remove(token)
                    except KeyError:
                        pass
            else:
                field = token
                if not field in result:
                    result[field] = set()

    return { k: list(v) for k, v in result.items() if v }

