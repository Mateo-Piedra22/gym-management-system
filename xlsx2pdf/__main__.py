import sys
from pathlib import Path

from . import convert


def main():
    if len(sys.argv) < 3:
        print("Uso: python -m xlsx2pdf <entrada.xlsx> <salida.pdf>")
        sys.exit(2)
    in_xlsx = Path(sys.argv[1]).resolve()
    out_pdf = Path(sys.argv[2]).resolve()
    try:
        convert(str(in_xlsx), str(out_pdf))
        print(f"PDF generado: {out_pdf}")
    except Exception as e:
        print(f"Error convirtiendo a PDF: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()