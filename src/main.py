import inspect as ins
import src.validation.validate_raw as vr

lista = []
for a, x in ins.getmembers(vr):
    lista.append(str(x))
print(lista)