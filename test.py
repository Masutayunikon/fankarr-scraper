import unicodedata
def norm(s):
    return "".join(
        c for c in unicodedata.normalize("NFD", s.lower())
        if unicodedata.category(c) != "Mn"
    )
print(norm("Kaï"))   # → "kai"
print(norm("Kai"))   # → "kai"