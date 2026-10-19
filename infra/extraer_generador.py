import json, sys

def buscar(obj, nivel=0):
    if nivel > 14:
        return None
    if isinstance(obj, str):
        t = obj.lstrip()
        if t.startswith("%%writefile"):
            return t.split("\n", 1)[1]
        if t[:1] in "{[" and t:
            try:
                return buscar(json.loads(t), nivel + 1)
            except ValueError:
                return None
        return None
    if isinstance(obj, dict):
        if "source" in obj and isinstance(obj["source"], list):
            r = buscar("".join(obj["source"]), nivel + 1)
            if r:
                return r
        for v in obj.values():
            r = buscar(v, nivel + 1)
            if r:
                return r
    if isinstance(obj, list):
        for v in obj:
            r = buscar(v, nivel + 1)
            if r:
                return r
    return None

raw = open(sys.argv[1], encoding="utf-8").read()
try:
    cod = buscar(json.loads(raw))
except ValueError:
    cod = None
if not cod:
    sys.exit("No encontré la celda %%writefile")
open("generador_dataset.py", "w", encoding="utf-8").write(cod)
print(f"generador_dataset.py escrito ({len(cod)} caracteres)")
